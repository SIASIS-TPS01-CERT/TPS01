import RDP02_DB_INSTANCES from "../../../connectors/postgres";



export async function upsertArchivoDatosAsistenciaDiariosEnBD(
  googleDriveId: string, nombreArchivo: string
) {
  const descripcion = `Archivo de respaldo. Actualizado el ${new Date().toLocaleString(
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
    nombreArchivo,
    googleDriveId,
    descripcion,
  ];

  return await RDP02_DB_INSTANCES.query(sql, params);
}
