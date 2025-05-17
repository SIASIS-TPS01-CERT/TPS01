import RDP02_DB_INSTANCES from "../../../connectors/postgres";

export async function verificarExistenciaRegistroDiario(
  tabla: string,
  campoDNI: string,
  dni: string,
  mes: number,
  dia: number,
  campoJson: "Entradas" | "Salidas"
): Promise<boolean> {
  try {
    const sql = `
      SELECT "${campoJson}"
      FROM "${tabla}"
      WHERE "${campoDNI}" = $1 AND "Mes" = $2
    `;

    const result = await RDP02_DB_INSTANCES.query(sql, [dni, mes]);

    if (result.rowCount > 0) {
      // El resultado puede tener el valor campoJson en minúsculas o en mayúsculas
      const jsonKey = Object.keys(result.rows[0]).find(
        (key) => key.toLowerCase() === campoJson.toLowerCase()
      );

      if (!jsonKey) {
        return false;
      }

      const jsonData = result.rows[0][jsonKey];

      // Verificar si ya existe un registro para este día
      return jsonData && jsonData[dia.toString()] !== undefined;
    }

    return false;
  } catch (error) {
    console.error(
      `Error al verificar existencia de registro diario en tabla ${tabla}:`,
      error
    );
    throw error;
  }
}
