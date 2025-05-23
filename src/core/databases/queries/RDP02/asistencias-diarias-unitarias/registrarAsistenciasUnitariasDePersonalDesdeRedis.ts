import { ModoRegistro } from "../../../../../interfaces/shared/ModoRegistroPersonal";
import { RolesSistema } from "../../../../../interfaces/shared/RolesSistema";
import { obtenerFechasActuales } from "../../../../utils/dates/obtenerFechasActuales";
import RDP02_DB_INSTANCES from "../../../connectors/postgres";
import { RegistroPersonalRedis } from "../../RDP05/obtenerRegistrosAsistenciaPersonalRedis";

// Interfaz para la configuración de tabla
interface ConfiguracionTabla {
  tabla: string;
  campoDNI: string;
  campoJSON: "Entradas" | "Salidas";
}

// Interfaz para el resultado del registro
interface ResultadoRegistro {
  registrosEntradaGuardados: number;
  registrosSalidaGuardados: number;
  registrosIgnorados: number;
  errores: string[];
}

/**
 * Obtiene la configuración de tabla y campos según el rol y modo de registro
 */
function obtenerConfiguracionTabla(
  rol: string,
  modoRegistro: string
): ConfiguracionTabla | null {
  const esEntrada = modoRegistro === ModoRegistro.Entrada;

  // Validar que el modo de registro sea válido
  if (!Object.values(ModoRegistro).includes(modoRegistro as ModoRegistro)) {
    console.warn(`⚠️  Modo de registro inválido: ${modoRegistro}`);
    return null;
  }

  switch (rol) {
    case RolesSistema.ProfesorPrimaria:
      return {
        tabla: esEntrada
          ? "T_Control_Entrada_Mensual_Profesores_Primaria"
          : "T_Control_Salida_Mensual_Profesores_Primaria",
        campoDNI: "DNI_Profesor_Primaria",
        campoJSON: esEntrada ? "Entradas" : "Salidas",
      };

    case RolesSistema.ProfesorSecundaria:
    case RolesSistema.Tutor:
      return {
        tabla: esEntrada
          ? "T_Control_Entrada_Mensual_Profesores_Secundaria"
          : "T_Control_Salida_Mensual_Profesores_Secundaria",
        campoDNI: "DNI_Profesor_Secundaria",
        campoJSON: esEntrada ? "Entradas" : "Salidas",
      };

    case RolesSistema.Auxiliar:
      return {
        tabla: esEntrada
          ? "T_Control_Entrada_Mensual_Auxiliar"
          : "T_Control_Salida_Mensual_Auxiliar",
        campoDNI: "DNI_Auxiliar",
        campoJSON: esEntrada ? "Entradas" : "Salidas",
      };

    case RolesSistema.PersonalAdministrativo:
      return {
        tabla: esEntrada
          ? "T_Control_Entrada_Mensual_Personal_Administrativo"
          : "T_Control_Salida_Mensual_Personal_Administrativo",
        campoDNI: "DNI_Personal_Administrativo",
        campoJSON: esEntrada ? "Entradas" : "Salidas",
      };

    default:
      console.warn(`⚠️  Rol no reconocido: ${rol}`);
      return null;
  }
}

/**
 * Función para registrar asistencia con valores personalizados (timestamp y desfase)
 */
async function registrarAsistenciaConValoresPersonalizados(
  tabla: string,
  campoDNI: string,
  dni: string,
  mes: number,
  dia: number,
  campoJson: "Entradas" | "Salidas",
  valorRegistro: { Timestamp: number; DesfaseSegundos: number },
  rol: string
): Promise<{ exito: boolean; mensaje: string }> {
  try {
    // Verificar si ya existe un registro para este mes
    // Para consultas de lectura, usamos la función normal (sin cache)
    const sqlVerificar = `
      SELECT *
      FROM "${tabla}"
      WHERE "${campoDNI}" = $1 AND "Mes" = $2
    `;
    const resultVerificar = await RDP02_DB_INSTANCES.query(
      sqlVerificar,
      [dni, mes],
      false
    );

    if (resultVerificar.rows.length > 0) {
      // Ya existe un registro para este mes, actualizarlo
      const registro = resultVerificar.rows[0];

      // Encontrar la clave correcta para el JSON
      const jsonKey = Object.keys(registro).find(
        (key) => key.toLowerCase() === campoJson.toLowerCase()
      );

      if (!jsonKey) {
        return {
          exito: false,
          mensaje: `Campo ${campoJson} no encontrado en el registro`,
        };
      }

      // Obtener el JSON actual o inicializar uno nuevo
      const jsonActual = registro[jsonKey] || {};

      // Solo actualizar si no existe un registro para este día
      if (jsonActual[dia.toString()] === undefined) {
        // Asignar el valor personalizado para este día
        jsonActual[dia.toString()] = valorRegistro;

        // Obtener el ID para actualización
        const idKey = Object.keys(registro).find((key) =>
          key.toLowerCase().includes("id")
        );

        if (!idKey) {
          return {
            exito: false,
            mensaje:
              "No se pudo determinar la columna ID para la actualización",
          };
        }

        // Actualizar el registro - como es una operación de escritura,
        // tu sistema automáticamente lo ejecutará en todas las instancias
        const sqlActualizar = `
          UPDATE "${tabla}"
          SET "${campoJson}" = $1
          WHERE "${idKey}" = $2
        `;
        await RDP02_DB_INSTANCES.query(sqlActualizar, [
          JSON.stringify(jsonActual),
          registro[idKey],
        ]);

        return {
          exito: true,
          mensaje: `Registro actualizado para DNI ${dni} en día ${dia}`,
        };
      } else {
        return {
          exito: false,
          mensaje: `Registro ya existe para DNI ${dni} en día ${dia}, manteniendo el existente`,
        };
      }
    } else {
      // No existe registro para este mes, crearlo
      const nuevoJson: any = {};
      nuevoJson[dia.toString()] = valorRegistro;

      // Insertar nuevo registro - como es una operación de escritura,
      // tu sistema automáticamente lo ejecutará en todas las instancias
      const sqlInsertar = `
        INSERT INTO "${tabla}" ("${campoDNI}", "Mes", "${campoJson}")
        VALUES ($1, $2, $3)
      `;
      await RDP02_DB_INSTANCES.query(sqlInsertar, [
        dni,
        mes,
        JSON.stringify(nuevoJson),
      ]);

      return {
        exito: true,
        mensaje: `Nuevo registro creado para DNI ${dni} en mes ${mes}`,
      };
    }
  } catch (error) {
    console.error(`❌ Error al registrar asistencia en tabla ${tabla}:`, error);
    return {
      exito: false,
      mensaje: `Error en base de datos: ${
        error instanceof Error ? error.message : String(error)
      }`,
    };
  }
}

/**
 * Persiste los registros de asistencia de personal de Redis en las tablas JSON mensuales correspondientes
 */
export async function registrarAsistenciasUnitariasDePersonalDesdeRedis(
  registros: RegistroPersonalRedis[]
): Promise<ResultadoRegistro> {
  try {
    console.log(
      "💾 Persistiendo registros de asistencia de personal en las tablas JSON mensuales..."
    );

    // Validación de entrada
    if (!Array.isArray(registros)) {
      throw new Error("El parámetro registros debe ser un array");
    }

    if (registros.length === 0) {
      console.log("ℹ️  No hay registros para procesar");
      return {
        registrosEntradaGuardados: 0,
        registrosSalidaGuardados: 0,
        registrosIgnorados: 0,
        errores: [],
      };
    }

    let registrosEntradaGuardados = 0;
    let registrosSalidaGuardados = 0;
    let registrosIgnorados = 0;
    const errores: string[] = [];

    // Obtener el mes y día usando tu función de fechas actuales
    // Esto respeta el mockeo y usa UTC correctamente
    const { fechaLocalPeru } = obtenerFechasActuales();
    const mesActual = fechaLocalPeru.getUTCMonth() + 1; // Usar UTC
    const diaActual = fechaLocalPeru.getUTCDate(); // Usar UTC

    console.log(
      `📅 Procesando registros para el mes ${mesActual}, día ${diaActual}`
    );
    console.log(`📊 Total de registros a procesar: ${registros.length}`);

    // Agrupar registros por DNI y modo para optimizar el procesamiento
    const registrosAgrupados = new Map<string, RegistroPersonalRedis[]>();

    for (const registro of registros) {
      const clave = `${registro.dni}-${registro.modoRegistro}`;
      if (!registrosAgrupados.has(clave)) {
        registrosAgrupados.set(clave, []);
      }
      registrosAgrupados.get(clave)!.push(registro);
    }

    console.log(
      `🔄 Procesando ${registrosAgrupados.size} grupos únicos de DNI-Modo`
    );

    // Procesar cada grupo de registros
    for (const [clave, grupoRegistros] of registrosAgrupados) {
      // Tomar el registro más reciente de cada grupo
      const registroMasReciente = grupoRegistros.reduce((max, current) =>
        current.timestamp > max.timestamp ? current : max
      );

      const { modoRegistro, rol, dni, timestamp, desfaseSegundos } =
        registroMasReciente;

      try {
        // Validar datos del registro
        if (!dni || !rol || !modoRegistro) {
          const error = `Datos faltantes en registro: DNI=${dni}, Rol=${rol}, Modo=${modoRegistro}`;
          console.warn(`⚠️  ${error}`);
          errores.push(error);
          registrosIgnorados++;
          continue;
        }

        // Obtener configuración de tabla
        const config = obtenerConfiguracionTabla(rol, modoRegistro);
        if (!config) {
          const error = `Configuración no encontrada para rol: ${rol}, modo: ${modoRegistro}`;
          console.warn(`⚠️  ${error}`);
          errores.push(error);
          registrosIgnorados++;
          continue;
        }

        const { tabla, campoDNI, campoJSON } = config;

        // Registrar con valores personalizados
        const resultado = await registrarAsistenciaConValoresPersonalizados(
          tabla,
          campoDNI,
          dni,
          mesActual,
          diaActual,
          campoJSON,
          {
            Timestamp: timestamp,
            DesfaseSegundos: desfaseSegundos,
          },
          rol
        );

        if (resultado.exito) {
          if (modoRegistro === ModoRegistro.Entrada) {
            registrosEntradaGuardados++;
          } else {
            registrosSalidaGuardados++;
          }
          console.log(`✅ ${resultado.mensaje}`);
        } else {
          console.log(`ℹ️  ${resultado.mensaje}`);
          registrosIgnorados++;
        }

        // Si había múltiples registros en el grupo, reportarlo
        if (grupoRegistros.length > 1) {
          console.log(
            `🔄 Se procesaron ${grupoRegistros.length} registros duplicados para ${dni}-${modoRegistro}, se tomó el más reciente`
          );
        }
      } catch (error) {
        const mensajeError = `Error al procesar registro para DNI ${dni}: ${
          error instanceof Error ? error.message : String(error)
        }`;
        console.error(`❌ ${mensajeError}`);
        errores.push(mensajeError);
        registrosIgnorados++;
      }
    }

    console.log(
      "\n=== 📊 Resumen de persistencia de registros de personal ==="
    );
    console.log(
      `📥 Registros de entrada guardados: ${registrosEntradaGuardados}`
    );
    console.log(
      `📤 Registros de salida guardados: ${registrosSalidaGuardados}`
    );
    console.log(`⏭️  Registros ignorados: ${registrosIgnorados}`);

    if (errores.length > 0) {
      console.log(`❌ Errores encontrados: ${errores.length}`);
      errores.slice(0, 5).forEach((error) => console.log(`   - ${error}`));
      if (errores.length > 5) {
        console.log(`   ... y ${errores.length - 5} errores más`);
      }
    }

    return {
      registrosEntradaGuardados,
      registrosSalidaGuardados,
      registrosIgnorados,
      errores,
    };
  } catch (error) {
    const mensajeError = `Error general al persistir registros: ${
      error instanceof Error ? error.message : String(error)
    }`;
    console.error(`❌ ${mensajeError}`);
    throw new Error(mensajeError);
  }
}
