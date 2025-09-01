
export async function descargarArchivoJSONDesdeGoogleDrive<T>(
  googleDriveId: string
): Promise<T> {
  try {
    const url = `https://drive.google.com/uc?export=download&id=${googleDriveId}`;
    const response = await fetch(url);
    const datos = await response.json();

    if (response.status !== 200) {
      throw new Error(
        `Error al descargar archivo de Google Drive. Código: ${response.status}`
      );
    }

    return datos;
  } catch (error) {
    // try {
    // const response = await fetch(process.env.);
    // const datosAsistencia = await response.json();
    // } catch (error) {
    console.error(
      "Error al descargar datos del archivo JSON desde Google Drive:",
      error
    );
    throw error;
    // }
  }
}
