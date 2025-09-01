import { obtenerFechasActuales } from "../../../../utils/dates/obtenerFechasActuales";
import { obtenerPersonalInactivoParaRegistroAutomatico } from "./obtenerPersonalInactivoParaRegistroAutomatico";
import RDP02_DB_INSTANCES from '../../../connectors/postgres';

export async function registrarAsistenciaAutoNullParaPersonalInactivo(): Promise<{
  totalRegistros: number;
  registrosCreados: number;
  registrosActualizados: number;
  errores: number;
}> {
  // Obtener fecha actual en Perú
  const { fechaLocalPeru } = obtenerFechasActuales();

  // Extraer mes y día
  const mes = fechaLocalPeru.getMonth() + 1; // getMonth() devuelve 0-11
  const dia = fechaLocalPeru.getDate();

  // Obtener lista de personal inactivo
  const personalInactivo =
    await obtenerPersonalInactivoParaRegistroAutomatico();

  console.log(`Personal inactivo encontrado: ${personalInactivo.length}`);

  let registrosCreados = 0;
  let registrosActualizados = 0;
  let errores = 0;

  if (personalInactivo.length === 0) {
    console.log("No se encontró personal inactivo para procesar");
    return {
      totalRegistros: 0,
      registrosCreados,
      registrosActualizados,
      errores,
    };
  }

  // Recopilar todas las tablas necesarias
  const tablasNecesarias = new Set<string>();
  personalInactivo.forEach((persona) => {
    tablasNecesarias.add(persona.tablaMensualEntrada);
    tablasNecesarias.add(persona.tablaMensualSalida);
  });

  // Verificar la existencia y obtener los nombres exactos de las tablas
  // Convertimos a formato con T_ y mayúsculas/minúsculas correctas
  const tablasFormateadas = Array.from(tablasNecesarias).map((t) => {
    // Si no comienza con T_ o t_, agregarlo
    const withPrefix = t.startsWith("T_") || t.startsWith("t_") ? t : `T_${t}`;

    // Convertir a formato PostgreSQL con primera letra de cada palabra en mayúscula
    return withPrefix
      .split("_")
      .map((word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
      .join("_");
  });

  // Consultamos las tablas existentes
  const sql = `
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public'
      AND (table_name = ANY($1) OR LOWER(table_name) = ANY($2))
    `;

  const tablasMinusculas = tablasFormateadas.map((t) => t.toLowerCase());
  const resultTablas = await RDP02_DB_INSTANCES.query(sql, [tablasFormateadas, tablasMinusculas]);

  // Crear un mapeo entre los nombres originales y los nombres reales
  const mapaTablas = new Map<string, string>();

  console.log(
    "Tablas encontradas en la base de datos:",
    resultTablas.rows.map((r: any) => r.table_name)
  );

  // Para cada nombre formateado, buscar su correspondiente en el resultado
  tablasFormateadas.forEach((tablaFormateada, index) => {
    const nombreOriginal = Array.from(tablasNecesarias)[index];
    const encontrada = resultTablas.rows.find(
      (row: any) =>
        row.table_name.toLowerCase() === tablaFormateada.toLowerCase()
    );

    if (encontrada) {
      mapaTablas.set(nombreOriginal, encontrada.table_name);
    }
  });

  console.log("Mapeo de tablas:", Object.fromEntries(mapaTablas));

  // Procesar cada miembro del personal inactivo
  for (const persona of personalInactivo) {
    // Verificar si la tabla de entrada existe
    const tablaEntradaReal = mapaTablas.get(persona.tablaMensualEntrada);
    if (tablaEntradaReal) {
      try {
        const resultadoEntrada = await verificarYRegistrarAsistenciaInactiva(
          tablaEntradaReal,
          persona.campoId,
          persona.campoIdUsuario,
          persona.id,
          mes,
          dia,
          "Entradas"
        );

        if (resultadoEntrada.creado) registrosCreados++;
        if (resultadoEntrada.actualizado) registrosActualizados++;
      } catch (error) {
        console.error(
          `Error al registrar entrada null para ${persona.id}:`,
          error
        );
        errores++;
      }
    } else {
      console.warn(
        `La tabla ${persona.tablaMensualEntrada} no existe en la base de datos. Omitiendo registro de entrada para ${persona.id}`
      );
    }

    // Verificar si la tabla de salida existe
    const tablaSalidaReal = mapaTablas.get(persona.tablaMensualSalida);
    if (tablaSalidaReal) {
      try {
        const resultadoSalida = await verificarYRegistrarAsistenciaInactiva(
          tablaSalidaReal,
          persona.campoId,
          persona.campoIdUsuario,
          persona.id,
          mes,
          dia,
          "Salidas"
        );

        if (resultadoSalida.creado) registrosCreados++;
        if (resultadoSalida.actualizado) registrosActualizados++;
      } catch (error) {
        console.error(
          `Error al registrar salida null para ${persona.id}:`,
          error
        );
        errores++;
      }
    } else {
      console.warn(
        `La tabla ${persona.tablaMensualSalida} no existe en la base de datos. Omitiendo registro de salida para ${persona.id}`
      );
    }
  }

  return {
    totalRegistros: personalInactivo.length * 2, // Entrada y salida para cada persona
    registrosCreados,
    registrosActualizados,
    errores,
  };
}

// Versión simplificada que usa directamente el nombre real de la tabla
async function verificarYRegistrarAsistenciaInactiva(
  tabla: string,
  campoId: string,
  campoIdUsuario: string,
  id: string,
  mes: number,
  dia: number,
  campoJson: "Entradas" | "Salidas"
): Promise<{ creado: boolean; actualizado: boolean }> {
  try {
    // Verificar si ya existe un registro para este mes
    const sqlVerificar = `
        SELECT "${campoId}", "${campoJson}"
        FROM "${tabla}"
        WHERE "${campoIdUsuario}" = $1 AND "Mes" = $2
      `;

    const resultVerificar = await RDP02_DB_INSTANCES.query(sqlVerificar, [id, mes]);

    if (resultVerificar.rowCount > 0) {
      // Ya existe un registro, actualizarlo para agregar el día actual como null
      const registro = resultVerificar.rows[0];
      // Asegurarnos de manejar correctamente el caso de columnas en minúsculas
      const jsonKey =
        Object.keys(registro).find(
          (k) => k.toLowerCase() === campoJson.toLowerCase()
        ) || campoJson;
      const idKey =
        Object.keys(registro).find(
          (k) => k.toLowerCase() === campoId.toLowerCase()
        ) || campoId;

      const jsonActual = registro[jsonKey] || {};

      // Si ya tiene un registro para este día, no hacer nada
      if (jsonActual[dia.toString()] !== undefined) {
        return { creado: false, actualizado: false };
      }

      // Agregar el registro null para el día actual
      jsonActual[dia.toString()] = null;

      // Actualizar el registro
      const sqlActualizar = `
          UPDATE "${tabla}"
          SET "${campoJson}" = $1
          WHERE "${campoId}" = $2
        `;

      await RDP02_DB_INSTANCES.query(sqlActualizar, [jsonActual, registro[idKey]]);
      return { creado: false, actualizado: true };
    } else {
      // No existe un registro, crearlo
      const nuevoJson: any = {};
      nuevoJson[dia.toString()] = null;

      const sqlInsertar = `
          INSERT INTO "${tabla}" ("${campoIdUsuario}", "Mes", "${campoJson}")
          VALUES ($1, $2, $3)
        `;

      await RDP02_DB_INSTANCES.query(sqlInsertar, [id, mes, nuevoJson]);
      return { creado: true, actualizado: false };
    }
  } catch (error) {
    console.error(
      `Error en verificarYRegistrarAsistenciaInactiva para tabla ${tabla}:`,
      error
    );
    throw error;
  }
}
