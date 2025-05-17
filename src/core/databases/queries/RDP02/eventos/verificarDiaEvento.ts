// src/core/database/queries/eventos/verificarDiaEvento.ts
import { T_Eventos } from "@prisma/client";
import RDP02_DB_INSTANCES from "../../../connectors/postgres";

export async function verificarDiaEvento(
  fecha: Date
): Promise<false | T_Eventos> {
  const fechaStr = fecha.toISOString().split("T")[0]; // Formato YYYY-MM-DD

  const sql = `
    SELECT *
    FROM "T_Eventos"
    WHERE "Fecha_Inicio" <= $1 AND "Fecha_Conclusion" >= $1
    LIMIT 1
  `;

  const result = await RDP02_DB_INSTANCES.query(sql, [fechaStr]);

  if (result.rows.length === 0) {
    return false;
  }

  return result.rows[0] as T_Eventos;
}
