// src/core/database/queries/personal/obtenerProfesoresPrimaria.ts
import { ProfesoresPrimariaParaTomaDeAsistencia } from "../../../../../interfaces/shared/Asistencia/DatosAsistenciaHoyIE20935";
import RDP02_DB_INSTANCES from "../../../connectors/postgres";

export async function obtenerProfesoresPrimariaParaTomarAsistencia(): Promise<
  ProfesoresPrimariaParaTomaDeAsistencia[]
> {
  const sql = `
    SELECT 
      "Id_Profesor_Primaria", 
      "Genero", 
      "Nombres", 
      "Apellidos", 
      "Google_Drive_Foto_ID"
    FROM "T_Profesores_Primaria"
    WHERE "Estado" = true
  `;

  const result = await RDP02_DB_INSTANCES.query(sql);

  return result.rows;
}
