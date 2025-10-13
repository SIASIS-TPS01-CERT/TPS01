import { NOMBRE_ARCHIVO_IDS_ARCHIVOS_ULTIMAS_ASISTENCIAS_DIARIAS } from "../../../../constants/NOMBRE_ARCHIVOS_SISTEMA";
import { ArchivoIdsArchivosUltimasAsistenciasEscolaresDiarias } from "../../../../interfaces/shared/Asistencia/ArchivoIdsArchivosUltimasAsistenciasEscolaresDiarias";
import { Fecha_ISO_8601 } from "../../../../interfaces/shared/Fechas";
import { NivelEducativo } from "../../../../interfaces/shared/NivelEducativo";
import { buscarArchivoRespaldoPorNombre } from "../../../databases/queries/RDP02/archivos-respaldo/buscarArchivoRespaldoPorNombre";
import { actualizarArchivoRespaldoEnGoogleDrive } from "../../../external/google/drive/actualizarArchivoDatosAsistencia";
import { deleteFileFromDrive } from "../../../external/google/drive/deleteFileFromDrive";
import { descargarArchivoJSONDesdeGoogleDrive } from "../../../external/google/drive/descargarArchivoJSONDesdeGoogle";

interface ResultadoGestionIds {
  archivoIdsActualizado: ArchivoIdsArchivosUltimasAsistenciasEscolaresDiarias;
  archivosEliminados: string[];
}

/**
 * Gestiona el archivo de IDs de archivos de asistencias diarias:
 * - Descarga o crea el archivo de IDs
 * - Agrega el nuevo ID del archivo de hoy
 * - Elimina archivos antiguos que excedan el límite
 * - Actualiza el archivo de IDs en Google Drive
 */
export async function gestionarArchivoIdsArchivosUltimasAsistenciasEscolaresDiarias(
  nivel: NivelEducativo,
  fechaISO: Fecha_ISO_8601, // Formato: "DD-MM-YYYY"
  nuevoArchivoId: string,
  limiteArchivos: number
): Promise<ResultadoGestionIds> {
  try {
    console.log(`\n🗂️ Gestionando archivo de IDs para ${nivel}...`);
    console.log(`   Fecha: ${fechaISO}`);
    console.log(`   Nuevo archivo ID: ${nuevoArchivoId}`);
    console.log(`   Límite de archivos: ${limiteArchivos}`);

    // 1. Buscar o crear archivo de IDs
    let archivoIds: ArchivoIdsArchivosUltimasAsistenciasEscolaresDiarias;
    let archivoExistente = await buscarArchivoRespaldoPorNombre(
      NOMBRE_ARCHIVO_IDS_ARCHIVOS_ULTIMAS_ASISTENCIAS_DIARIAS
    );

    if (archivoExistente && archivoExistente.Google_Drive_Id) {
      console.log("   📥 Descargando archivo de IDs existente...");
      archivoIds =
        await descargarArchivoJSONDesdeGoogleDrive<ArchivoIdsArchivosUltimasAsistenciasEscolaresDiarias>(
          archivoExistente.Google_Drive_Id
        );
    } else {
      console.log("   📝 Creando nuevo archivo de IDs...");
      archivoIds = {
        [NivelEducativo.PRIMARIA]: {},
        [NivelEducativo.SECUNDARIA]: {},
      };
    }

    // 2. Obtener registros del nivel actual
    const registrosNivel = archivoIds[nivel] || {};

    // 3. Agregar nuevo archivo
    registrosNivel[fechaISO] = nuevoArchivoId;
    console.log(`   ✅ Nuevo archivo agregado para fecha ${fechaISO}`);

    // 4. Ordenar fechas (más reciente primero) y limpiar antiguos
    const fechasOrdenadas = Object.keys(registrosNivel).sort((a, b) => {
      // Convertir DD-MM-YYYY a Date para comparar
      const [diaA, mesA, anioA] = a.split("-").map(Number);
      const [diaB, mesB, anioB] = b.split("-").map(Number);
      const dateA = new Date(anioA, mesA - 1, diaA);
      const dateB = new Date(anioB, mesB - 1, diaB);
      return dateB.getTime() - dateA.getTime(); // Más reciente primero
    });

    console.log(`   📊 Total de fechas registradas: ${fechasOrdenadas.length}`);
    console.log(`   🎯 Límite de archivos permitidos: ${limiteArchivos}`);

    // 5. Identificar archivos a eliminar
    const archivosAEliminar = fechasOrdenadas.slice(limiteArchivos);
    const archivosEliminados: string[] = [];

    if (archivosAEliminar.length > 0) {
      console.log(
        `   🗑️ Eliminando ${archivosAEliminar.length} archivos antiguos...`
      );

      for (const fechaAntigua of archivosAEliminar) {
        const idArchivoAntiguo = registrosNivel[fechaAntigua as Fecha_ISO_8601];

        console.log(
          `      - Eliminando archivo de fecha ${fechaAntigua} (ID: ${idArchivoAntiguo})`
        );

        try {
          await deleteFileFromDrive(idArchivoAntiguo);
          delete registrosNivel[fechaAntigua as Fecha_ISO_8601];
          archivosEliminados.push(idArchivoAntiguo);
        } catch (error) {
          console.error(
            `      ❌ Error eliminando archivo ${idArchivoAntiguo}:`,
            error
          );
        }
      }

      console.log(
        `   ✅ ${archivosEliminados.length} archivos eliminados exitosamente`
      );
    } else {
      console.log(`   ✅ No hay archivos antiguos para eliminar`);
    }

    // 6. Actualizar archivo de IDs
    archivoIds[nivel] = registrosNivel;

    console.log(`   💾 Actualizando archivo de IDs en Google Drive...`);
    await actualizarArchivoRespaldoEnGoogleDrive(
      NOMBRE_ARCHIVO_IDS_ARCHIVOS_ULTIMAS_ASISTENCIAS_DIARIAS,
      archivoIds,
      "Archivos de Respaldo"
    );

    console.log(`   ✅ Archivo de IDs actualizado correctamente`);

    // Resumen final
    const archivosRestantes = Object.keys(registrosNivel).length;
    console.log(`\n   📋 Resumen para ${nivel}:`);
    console.log(`      - Archivos actuales: ${archivosRestantes}`);
    console.log(`      - Archivos eliminados: ${archivosEliminados.length}`);

    return {
      archivoIdsActualizado: archivoIds,
      archivosEliminados,
    };
  } catch (error) {
    console.error("❌ Error gestionando archivo de IDs:", error);
    throw error;
  }
}
