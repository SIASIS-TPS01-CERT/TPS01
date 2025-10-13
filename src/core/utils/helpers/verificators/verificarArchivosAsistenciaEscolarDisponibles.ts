import { ArchivoIdsArchivosUltimasAsistenciasEscolaresDiarias } from "../../../../interfaces/shared/Asistencia/ArchivoIdsArchivosUltimasAsistenciasEscolaresDiarias";
import { descargarArchivoJSONDesdeGoogleDrive } from "../../../external/google/drive/descargarArchivoJSONDesdeGoogle";
import { NOMBRE_ARCHIVO_IDS_ARCHIVOS_ULTIMAS_ASISTENCIAS_DIARIAS } from "../../../../constants/NOMBRE_ARCHIVOS_SISTEMA";
import { NivelEducativo } from "../../../../interfaces/shared/NivelEducativo";
import { Fecha_ISO_8601 } from "../../../../interfaces/shared/Fechas";
import { buscarArchivoRespaldoPorNombre } from "../../../databases/queries/RDP02/archivos-respaldo/buscarArchivoRespaldoPorNombre";
import { parsearFechaDesdeArchivoIds } from "../formatters/formatearFechasParaArchivosJSON";

export interface ArchivoAsistenciaEscolarReciente {
  fecha: Date;
  fechaISO8601: Fecha_ISO_8601; // Formato: DD-MM-YYYY
  googleDriveId: string;
}

export async function verificarArchivosAsistenciaEscolarDisponibles(
  nivel: NivelEducativo,
  faltasConsecutivasMaximas: number,
  tardanzasConsecutivasMaximas: number
): Promise<{
  archivosDisponibles: ArchivoAsistenciaEscolarReciente[];
  suficientesParaFaltas: boolean;
  suficientesParaTardanzas: boolean;
}> {
  try {
    console.log(`   🔍 Buscando archivo índice en Google Drive...`);

    // 1. Buscar el archivo índice en la base de datos
    const archivoIndice = await buscarArchivoRespaldoPorNombre(
      NOMBRE_ARCHIVO_IDS_ARCHIVOS_ULTIMAS_ASISTENCIAS_DIARIAS
    );

    if (!archivoIndice || !archivoIndice.Google_Drive_Id) {
      console.log(
        `   ⚠️ No se encontró archivo índice. No hay archivos disponibles.`
      );
      return {
        archivosDisponibles: [],
        suficientesParaFaltas: false,
        suficientesParaTardanzas: false,
      };
    }

    console.log(`   📥 Descargando archivo índice desde Google Drive...`);
    console.log(`      ID: ${archivoIndice.Google_Drive_Id}`);

    // 2. Descargar el archivo índice
    const datosIndice =
      await descargarArchivoJSONDesdeGoogleDrive<ArchivoIdsArchivosUltimasAsistenciasEscolaresDiarias>(
        archivoIndice.Google_Drive_Id
      );

    // 3. Obtener registros del nivel correspondiente
    const registrosNivel = datosIndice[nivel] || {};
    const fechasRegistradas = Object.keys(registrosNivel) as Fecha_ISO_8601[];

    console.log(
      `   📊 Archivos encontrados para ${nivel}: ${fechasRegistradas.length}`
    );

    if (fechasRegistradas.length === 0) {
      console.log(`   ℹ️ No hay archivos de asistencia para ${nivel}`);
      return {
        archivosDisponibles: [],
        suficientesParaFaltas: false,
        suficientesParaTardanzas: false,
      };
    }

    // 4. Ordenar fechas (más reciente primero)
    const archivosOrdenados: ArchivoAsistenciaEscolarReciente[] =
      fechasRegistradas
        .map((fechaISO8601) => ({
          fecha: parsearFechaDesdeArchivoIds(fechaISO8601),
          fechaISO8601: fechaISO8601,
          googleDriveId: registrosNivel[fechaISO8601],
        }))
        .sort((a, b) => b.fecha.getTime() - a.fecha.getTime()); // Más reciente primero

    // Log de archivos encontrados (mostrar primeros 5)
    console.log(`   📋 Archivos disponibles (más recientes primero):`);
    archivosOrdenados.slice(0, 5).forEach((archivo, idx) => {
      console.log(
        `      ${idx + 1}. ${
          archivo.fechaISO8601
        } (${archivo.googleDriveId.substring(0, 15)}...)`
      );
    });
    if (archivosOrdenados.length > 5) {
      console.log(`      ... y ${archivosOrdenados.length - 5} archivos más`);
    }

    // 5. Verificar si hay suficientes archivos
    const cantidadArchivos = archivosOrdenados.length;
    const suficientesParaFaltas = cantidadArchivos >= faltasConsecutivasMaximas;
    const suficientesParaTardanzas =
      cantidadArchivos >= tardanzasConsecutivasMaximas;

    console.log(
      `   ✓ Suficientes para analizar faltas (${faltasConsecutivasMaximas} requeridos): ${
        suficientesParaFaltas ? "✅ Sí" : "❌ No"
      }`
    );
    console.log(
      `   ✓ Suficientes para analizar tardanzas (${tardanzasConsecutivasMaximas} requeridos): ${
        suficientesParaTardanzas ? "✅ Sí" : "❌ No"
      }`
    );

    return {
      archivosDisponibles: archivosOrdenados,
      suficientesParaFaltas,
      suficientesParaTardanzas,
    };
  } catch (error) {
    console.error("❌ Error verificando archivos disponibles:", error);
    throw error;
  }
}
