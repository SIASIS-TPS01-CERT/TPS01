import { put } from "@vercel/blob";

import { RDP04_INSTANCES_DISTRIBUTION } from "../../../../constants/RDP04_INSTANCES_DISTRIBUTION";

export async function guardarObjetoComoJSONEnBlobs(
  datos: any,
  nombreArchivoJson: string
) {
  const resultados = [];
  const errores = [];

  try {
    // Contenido JSON que se guardará
    const contenidoJSON = JSON.stringify(datos);

    // Guardar en cada blob
    for (const config of RDP04_INSTANCES_DISTRIBUTION) {
      try {
        // Verificar que existe el token de entorno
        const token = process.env[config.tokenEnv];
        if (!token) {
          console.warn(
            `Token no encontrado para ${config.nombre}: ${config.tokenEnv}`
          );
          errores.push({
            nombre: config.nombre,
            error: `Token no configurado (${config.tokenEnv})`,
          });
          continue;
        }

        // Guardar en el blob
        const blob = await put(nombreArchivoJson, contenidoJSON, {
          access: "public",
          addRandomSuffix: false,
          token: token,
        });

        console.log(`Datos guardados en Blob ${config.nombre}: ${blob.url}`);
        resultados.push({ nombre: config.nombre, url: blob.url });
      } catch (error) {
        console.error(`Error al guardar en Blob ${config.nombre}:`, error);
        errores.push({
          nombre: config.nombre,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    }

    // Mostrar resumen de operación
    console.log(`=== Resumen de guardado en blobs ===`);
    console.log(`- Éxitos: ${resultados.length}`);
    console.log(`- Errores: ${errores.length}`);

    // Si no se guardó en ninguno, lanzar error
    if (resultados.length === 0) {
      throw new Error(
        `No se pudo guardar los datos en ningún blob. Errores: ${JSON.stringify(
          errores
        )}`
      );
    }

    return {
      resultados,
      errores,
      totalExitosos: resultados.length,
      totalErrores: errores.length,
    };
  } catch (error) {
    console.error("Error al guardar datos en Vercel Blobs:", error);
    throw error;
  }
}
