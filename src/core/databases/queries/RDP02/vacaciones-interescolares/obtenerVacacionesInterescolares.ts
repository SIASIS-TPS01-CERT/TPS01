import RDP02_DB_INSTANCES from '../../../connectors/postgres';

export async function obtenerVacacionesInterescolares() {
  const sql = `
    SELECT 
      "Id_Vacacion_Interescolar",
      "Fecha_Inicio",
      "Fecha_Conclusion"
    FROM "T_Vacaciones_Interescolares"
    ORDER BY "Fecha_Inicio" ASC
  `;

  const result = await RDP02_DB_INSTANCES.query(sql);
  return result.rows;
}