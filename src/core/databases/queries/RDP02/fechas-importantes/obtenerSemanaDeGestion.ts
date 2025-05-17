// obtenerSemanaDeGestion.ts
import { RangoFechas } from "../../../../../interfaces/shared/Asistencia/DatosAsistenciaHoyIE20935";
import { query } from "../../../connectors/postgres";
import RDP02_DB_INSTANCES from "../../../connectors/postgres";

export async function obtenerSemanaDeGestion(): Promise<RangoFechas | null> {
  // Consulta para verificar primero si existen los registros
  const checkSql = `
    SELECT COUNT(*) as count
    FROM "T_Fechas_Importantes" 
    WHERE "Nombre" IN ('Fecha_Inicio_Semana_Gestion', 'Fecha_Fin_Semana_Gestion')
  `;

  const checkResult = await query(checkSql);

  // Si no hay exactamente 2 registros (inicio y fin), entonces no hay semana de gestión configurada
  if (checkResult.rows[0].count < 2) {
    return null;
  }

  // Si existen los registros, procedemos a obtener los valores
  const sql = `
    SELECT 
      (SELECT "Valor" FROM "T_Fechas_Importantes" WHERE "Nombre" = 'Fecha_Inicio_Semana_Gestion' LIMIT 1) as "Fecha_Inicio",
      (SELECT "Valor" FROM "T_Fechas_Importantes" WHERE "Nombre" = 'Fecha_Fin_Semana_Gestion' LIMIT 1) as "Fecha_Fin"
  `;

  const result = await RDP02_DB_INSTANCES.query(sql);

  // Verificación adicional para asegurarnos que ambos valores existen
  if (!result.rows[0].Fecha_Inicio || !result.rows[0].Fecha_Fin) {
    return null;
  }

  return {
    Inicio: result.rows[0].Fecha_Inicio,
    Fin: result.rows[0].Fecha_Fin,
  };
}
