import { ProfesoresPrimariaParaTomaDeAsistencia } from "../../../../../interfaces/shared/Asistencia/DatosAsistenciaHoyIE20935";
import RDP02_DB_INSTANCES from "../../../connectors/postgres";

export async function obtenerProfesoresPrimariaParaTomarAsistencia(): Promise<
  ProfesoresPrimariaParaTomaDeAsistencia[]
> {
  const sql = `
    SELECT 
      pp."Id_Profesor_Primaria", 
      pp."Genero", 
      pp."Nombres", 
      pp."Apellidos", 
      pp."Google_Drive_Foto_ID",
      a."Id_Aula",
      a."Color",
      a."Grado",
      a."Nivel",
      a."Seccion"
    FROM "T_Profesores_Primaria" pp
    LEFT JOIN "T_Aulas" a ON pp."Id_Profesor_Primaria" = a."Id_Profesor_Primaria"
    WHERE pp."Estado" = true
  `;

  const result = await RDP02_DB_INSTANCES.query(sql);

  // Transformar los resultados para incluir el objeto Aula
  return result.rows.map((row: any) => ({
    Id_Profesor_Primaria: row.Id_Profesor_Primaria,
    Genero: row.Genero,
    Nombres: row.Nombres,
    Apellidos: row.Apellidos,
    Google_Drive_Foto_ID: row.Google_Drive_Foto_ID,
    Aula: row.Id_Aula
      ? {
          Id_Aula: row.Id_Aula,
          Color: row.Color,
          Grado: row.Grado,
          Nivel: row.Nivel,
          Seccion: row.Seccion,
        }
      : null,
  }));
}
