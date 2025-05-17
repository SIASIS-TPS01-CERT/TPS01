import { NOMBRE_ARCHIVO_CON_DATOS_ASISTENCIA_DIARIOS } from "../../../../../constants/NOMBRE_ARCHIVOS_SISTEMA";
import RDP02_DB_INSTANCES from "../../../connectors/postgres";

/**
 * Busca el archivo de datos de asistencia en la base de datos
 * @returns Información del archivo si existe, null si no existe
 */
export async function buscarArchivoDatosAsistenciaDiariosEnBD() {
  const sql = `
    SELECT * FROM "T_Archivos_Respaldo_Google_Drive"
    WHERE "Nombre_Archivo" = $1
  `;

  const result = await RDP02_DB_INSTANCES.query(sql, [
    NOMBRE_ARCHIVO_CON_DATOS_ASISTENCIA_DIARIOS,
  ]);

  if (result.rows.length > 0) {
    return result.rows[0];
  }

  return null;
}

export async function upsertArchivoDatosAsistenciaDiariosEnBD(
  googleDriveId: string
) {
  const descripcion = `Archivo con datos de asistencia diaria. Actualizado el ${new Date().toLocaleString(
    "es-PE",
    { timeZone: "America/Lima" }
  )}`;

  const sql = `
    INSERT INTO "T_Archivos_Respaldo_Google_Drive" 
    ("Nombre_Archivo", "Google_Drive_Id", "Descripcion")
    VALUES ($1, $2, $3)
    ON CONFLICT ("Nombre_Archivo") DO UPDATE 
    SET 
      "Google_Drive_Id" = $2,
      "Descripcion" = $3,
      "Ultima_Modificacion" = CURRENT_TIMESTAMP
    RETURNING *
  `;

  const params = [
    NOMBRE_ARCHIVO_CON_DATOS_ASISTENCIA_DIARIOS,
    googleDriveId,
    descripcion,
  ];

  return await RDP02_DB_INSTANCES.query(sql, params);
}
