import { AuxiliaresParaTomaDeAsistencia } from "../../../../../interfaces/shared/Asistencia/DatosAsistenciaHoyIE20935";
import RDP02_DB_INSTANCES from "../../../connectors/postgres";

export async function obtenerAuxiliaresParaTomarAsistencia(): Promise<
  AuxiliaresParaTomaDeAsistencia[]
> {
  const sql = `
    SELECT 
      "DNI_Auxiliar", 
      "Nombres", 
      "Apellidos", 
      "Genero", 
      "Google_Drive_Foto_ID"
    FROM "T_Auxiliares"
    WHERE "Estado" = true
  `;

  const result = await RDP02_DB_INSTANCES.query(sql);
  return result.rows;
}
