import { T_Archivos_Respaldo_Google_Drive } from "@prisma/client";
import RDP02_DB_INSTANCES from "../../../connectors/postgres";

/**
 * Obtiene todos los archivos existentes de listas de estudiantes desde PostgreSQL
 */
export async function obtenerArchivosRespaldoDeUltimasListasEstudiantes(): Promise<
  T_Archivos_Respaldo_Google_Drive[]
> {
  try {
    const sql = `
      SELECT "Id_Archivo_Respaldo", "Nombre_Archivo", "Google_Drive_Id", "Descripcion", "Ultima_Modificacion"
      FROM "T_Archivos_Respaldo_Google_Drive"
      WHERE "Nombre_Archivo" LIKE 'Estudiantes_%.json'
      ORDER BY "Nombre_Archivo" ASC
    `;

    const result = await RDP02_DB_INSTANCES.query(sql);

    console.log(
      `📁 Encontrados ${result.rowCount} archivos de estudiantes existentes`
    );
    return result.rows || [];
  } catch (error) {
    console.error(
      "Error al obtener archivos de estudiantes existentes:",
      error
    );
    return [];
  }
}
