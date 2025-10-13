import RDP02_DB_INSTANCES from '../../../connectors/postgres';

export async function registrarAsistenciaConValoresNull(
  tabla: string,
  campoID: string,
  id: string,
  mes: number,
  dia: number,
  campoJson: "Entradas" | "Salidas"
): Promise<void> {
  try {
    // Verificar si ya existe un registro para este mes
    const sqlVerificar = `
        SELECT *
        FROM "${tabla}"
        WHERE "${campoID}" = $1 AND "Mes" = $2
      `;

    const resultVerificar = await RDP02_DB_INSTANCES.query(sqlVerificar, [id, mes]);

    // Objeto con valores nulos
    const entradaNula = {
      Timestamp: null,
      DesfaseSegundos: null,
    };

    if (resultVerificar.rowCount > 0) {
      // Ya existe un registro para este mes, actualizarlo
      const registro = resultVerificar.rows[0];

      // Encontrar la clave correcta para el JSON (puede estar en minúsculas)
      const jsonKey = Object.keys(registro).find(
        (key) => key.toLowerCase() === campoJson.toLowerCase()
      );

      if (!jsonKey) {
        throw new Error(`Campo ${campoJson} no encontrado en el registro`);
      }

      // Obtener el ID para actualización
      const idKey = Object.keys(registro).find(
        (key) =>
          key.toLowerCase().includes("id") && key.toLowerCase().includes("p")
      );

      if (!idKey) {
        throw new Error(
          "No se pudo determinar la columna ID para la actualización"
        );
      }

      // Obtener el JSON actual o inicializar uno nuevo
      const jsonActual = registro[jsonKey] || {};

      // Solo actualizar si no existe un registro para este día
      if (jsonActual[dia.toString()] === undefined) {
        // Asignar entrada nula para este día
        jsonActual[dia.toString()] = entradaNula;

        // Actualizar el registro
        const sqlActualizar = `
            UPDATE "${tabla}"
            SET "${campoJson}" = $1
            WHERE "${idKey}" = $2
          `;

        await RDP02_DB_INSTANCES.query(sqlActualizar, [jsonActual, registro[idKey]]);
      }
    } else {
      // No existe registro para este mes, crearlo
      const nuevoJson: any = {};
      nuevoJson[dia.toString()] = entradaNula;

      const sqlInsertar = `
          INSERT INTO "${tabla}" ("${campoID}", "Mes", "${campoJson}")
          VALUES ($1, $2, $3)
        `;

      await RDP02_DB_INSTANCES.query(sqlInsertar, [id, mes, nuevoJson]);
    }
  } catch (error) {
    console.error(
      `Error al registrar asistencia con valores null en tabla ${tabla}:`,
      error
    );
    throw error;
  }
}
