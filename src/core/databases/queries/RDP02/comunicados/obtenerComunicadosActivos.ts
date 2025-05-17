import { T_Comunicados } from "@prisma/client";
import RDP02_DB_INSTANCES from "../../../connectors/postgres";

export async function obtenerComunicadosActivos(
  fecha: Date
): Promise<T_Comunicados[]> {
  const fechaStr = fecha.toISOString().split("T")[0]; // Formato YYYY-MM-DD

  const sql = `
    SELECT *
    FROM "T_Comunicados"
    WHERE "Fecha_Inicio" <= $1 AND "Fecha_Conclusion" >= $1
    ORDER BY "Fecha_Inicio" DESC
  `;

  const result = await RDP02_DB_INSTANCES.query(sql, [fechaStr]);

  return result.rows;
}
