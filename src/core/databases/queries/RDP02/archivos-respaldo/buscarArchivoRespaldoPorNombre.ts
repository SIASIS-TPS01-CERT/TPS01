import { T_Archivos_Respaldo_Google_Drive } from "@prisma/client";
import RDP02_DB_INSTANCES from "../../../connectors/postgres";

/**
 * Busca el archivo de datos de asistencia en la base de datos
 * @returns Información del archivo si existe, null si no existe
 */
export async function buscarArchivoRespaldoPorNombre(nombreArchivo: string):Promise<T_Archivos_Respaldo_Google_Drive|null> {
  const sql = `
    SELECT * FROM "T_Archivos_Respaldo_Google_Drive"
    WHERE "Nombre_Archivo" = $1
  `;

  const result = await RDP02_DB_INSTANCES.query(sql, [nombreArchivo]);

  if (result.rows.length > 0) {
    return result.rows[0];
  }

  return null;
}
