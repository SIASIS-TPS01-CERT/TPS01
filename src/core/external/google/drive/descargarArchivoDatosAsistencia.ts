import { DatosAsistenciaHoyIE20935 } from "../../../../interfaces/shared/Asistencia/DatosAsistenciaHoyIE20935";

export async function descargarArchivoDatosAsistenciaDesdeGoogleDrive(
  googleDriveId: string
): Promise<DatosAsistenciaHoyIE20935> {
  try {
    const url = `https://drive.google.com/uc?export=download&id=${googleDriveId}`;
    const response = await fetch(url);
    const datosAsistencia = await response.json();

    if (response.status !== 200) {
      throw new Error(
        `Error al descargar archivo de Google Drive. Código: ${response.status}`
      );
    }

    return datosAsistencia;
  } catch (error) {
    console.error(
      "Error al descargar datos de asistencia desde Google Drive:",
      error
    );
    throw error;
  }
}
