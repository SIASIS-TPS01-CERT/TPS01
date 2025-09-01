import { redisClient } from "../../../../config/Redis/RedisClient";
import { buscarArchivoRespaldoPorNombre } from "../../../databases/queries/RDP02/archivos-respaldo/buscarArchivoRespaldoPorNombre";
import { upsertArchivoDatosAsistenciaDiariosEnBD } from "../../../databases/queries/RDP02/asistencia-diaria/operacionesAsistenciaDiaria";
import { deleteFileFromDrive } from "./deleteFileFromDrive";
import { uploadJsonToDrive } from "./uploadJsonToDrive";

/**
 * Gestiona el ciclo completo de actualización del archivo de datos de asistencia:
 * 1. Busca si existe un archivo previo
 * 2. Elimina el archivo anterior de Google Drive (si existe)
 * 3. Sube el nuevo archivo
 * 4. Actualiza los registros en la base de datos
 *
 * @param jsonData Datos de asistencia que se guardarán en el archivo JSON
 * @param folderPath Ruta de carpetas en Google Drive donde se almacenará el archivo
 * @returns Información del nuevo archivo creado
 */
export async function actualizarArchivoRespaldoEnGoogleDrive(
  nombreArchivo: string,
  jsonData: any,
  folderPath: string = "Archivos de Respaldo"
) {
  try {
    console.log(
      "Iniciando actualización del archivo de datos de asistencia..."
    );

    // 1. Buscar si existe un archivo previo
    const archivoExistente = await buscarArchivoRespaldoPorNombre(
      nombreArchivo
    );
    console.log("Archivo existente en BD:", archivoExistente ? "SÍ" : "NO");

    // 2. Eliminar el archivo anterior de Google Drive (si existe)
    if (archivoExistente && archivoExistente.Google_Drive_Id) {
      console.log(
        `Eliminando archivo anterior con ID: ${archivoExistente.Google_Drive_Id}`
      );
      await deleteFileFromDrive(archivoExistente.Google_Drive_Id);
    }

    // 3. Subir el nuevo archivo
    console.log("Subiendo nuevo archivo con datos actualizados...");
    const nuevoArchivo = await uploadJsonToDrive(
      jsonData,
      folderPath,
      nombreArchivo
    );

    // 4. Guardar archivo en todas las
    await redisClient().set(nombreArchivo, nuevoArchivo.id);

    // 5. Actualizar los registros en la base de datos
    console.log(`Archivo subido con éxito. Nuevo ID: ${nuevoArchivo.id}`);
    const registroBD = await upsertArchivoDatosAsistenciaDiariosEnBD(
      nuevoArchivo.id,
      nombreArchivo
    );

    console.log("Registro en BD actualizado:", registroBD);
    return {
      archivo: nuevoArchivo,
      registro: registroBD,
    };
  } catch (error) {
    console.error("Error al actualizar archivo de datos de asistencia:", error);
    throw error;
  }
}
