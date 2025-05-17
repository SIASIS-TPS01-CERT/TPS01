import fs from "fs";
import path from "path";

import { getDriveClient } from "../../../../config/GoogleDrive/getDriveClient";
import { findOrCreateFolder } from "./findOrCreateFolder";

/**
 * Sube un objeto JSON a Google Drive
 * @param jsonData El objeto que se convertirá a JSON y se subirá
 * @param folderPath Ruta de carpetas en Google Drive (ej: "Asistencia/Diaria")
 * @param fileName Nombre del archivo a crear
 * @returns Información del archivo creado
 */
export async function uploadJsonToDrive(
  jsonData: any,
  folderPath: string,
  fileName: string
) {
  try {
    const drive = await getDriveClient();

    // Buscar o crear la carpeta
    const folderId = await findOrCreateFolder(drive, folderPath);

    // Convertir el objeto a string JSON
    const jsonContent = JSON.stringify(jsonData, null, 2);

    // Crear un archivo temporal
    const tempFilePath = path.join(__dirname, "temp_" + fileName);
    fs.writeFileSync(tempFilePath, jsonContent);

    // Configurar la solicitud de carga
    const fileMetadata = {
      name: fileName,
      parents: [folderId],
      mimeType: "application/json",
    };

    const media = {
      mimeType: "application/json",
      body: fs.createReadStream(tempFilePath),
    };

    // Subir el archivo
    const response = await drive.files.create({
      requestBody: fileMetadata,
      media: media,
      fields: "id,webViewLink,webContentLink",
    });

    // Eliminar el archivo temporal
    fs.unlinkSync(tempFilePath);

    // Configurar permisos para que sea accesible
    await drive.permissions.create({
      fileId: response.data.id!,
      requestBody: {
        role: "reader",
        type: "anyone",
      },
    });

    return {
      id: response.data.id!,
      webViewLink: response.data.webViewLink!,
      webContentLink: response.data.webContentLink!,
    };
  } catch (error) {
    console.error("Error al subir archivo JSON a Google Drive:", error);
    throw error;
  }
}
