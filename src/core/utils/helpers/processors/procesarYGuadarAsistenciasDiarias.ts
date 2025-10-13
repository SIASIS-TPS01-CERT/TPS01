import { obtenerNombreArchivoAsistenciaEscolarDiaria } from "../../../../interfaces/shared/Asistencia/ArchivoAsistenciaEscolarDiaria";
import { NivelEducativo } from "../../../../interfaces/shared/NivelEducativo";
import {
  EstudianteActivoSecundaria,
  RegistroEstudianteSecundariaRedis,
} from "../../../../jobs/asistenciaEscolar/SetAsistenciasYFaltasEstudiantesSecundaria";
import { obtenerLimiteTardanzasFaltasEscolaresConsecutivas } from "../../../databases/queries/RDP02/ajustes-generales/obtenerLimiteTardanzasFaltasEscolaresConsecutivas";
import { uploadJsonToDrive } from "../../../external/google/drive/uploadJsonToDrive";
import { construirAsistenciasEscolaresDelDia } from "../builders/construirAsistenciasEscolaresDelDia";
import { formatearFechaParaArchivoIds } from "../formatters/formatearFechasParaArchivosJSON";
import { gestionarArchivoIdsArchivosUltimasAsistenciasEscolaresDiarias } from "../managers/gestionarArchivoIdsArchivosUltimasAsistenciasEscolaresDiarias";

interface ParametrosProcesamiento {
  estudiantesActivos: EstudianteActivoSecundaria[];
  registrosRedis: RegistroEstudianteSecundariaRedis[];
  nivel: NivelEducativo;
  fechaActual: Date;
}

interface ResultadoProcesamiento {
  archivoCreado: {
    id: string;
    nombre: string;
  };
  archivosEliminados: string[];
}

/**
 * Función principal que orquesta todo el proceso de:
 * 1. Construir el archivo JSON de asistencias del día
 * 2. Subir el archivo a Google Drive
 * 3. Gestionar el archivo de IDs (agregar nuevo, eliminar antiguos)
 */
/**
 * Función principal que orquesta todo el proceso de:
 * 1. Construir el archivo JSON de asistencias del día
 * 2. Subir el archivo a Google Drive
 * 3. Gestionar el archivo de IDs (agregar nuevo, eliminar antiguos)
 *
 * NOTA: Esta función debe llamarse DESPUÉS de desbloquear los roles
 * para no prolongar el tiempo de bloqueo
 */
export async function procesarYGuardarAsistenciasDiarias(
  params: ParametrosProcesamiento
): Promise<ResultadoProcesamiento> {
  try {
    const { estudiantesActivos, registrosRedis, nivel, fechaActual } = params;

    console.log("\n" + "=".repeat(70));
    console.log("📦 PROCESAMIENTO DE ARCHIVO DE ASISTENCIAS DIARIAS");
    console.log("=".repeat(70));

    // 1. Obtener configuración de límite de archivos
    console.log("\n🔧 PASO 1: Obteniendo configuración de límites...");
    const config = await obtenerLimiteTardanzasFaltasEscolaresConsecutivas(
      nivel
    );

    // 2. Construir objeto de asistencias del día
    console.log("\n📝 PASO 2: Construyendo archivo de asistencias...");
    const archivoAsistencias = construirAsistenciasEscolaresDelDia({
      estudiantesActivos,
      registrosRedis,
      nivel,
      fechaActual,
    });

    // 3. Generar nombre del archivo
    const nombreArchivo = obtenerNombreArchivoAsistenciaEscolarDiaria(
      fechaActual,
      nivel
    );
    console.log(`   📄 Nombre del archivo: ${nombreArchivo}`);

    // 4. Subir archivo a Google Drive
    console.log("\n☁️ PASO 3: Subiendo archivo a Google Drive...");
    const archivoSubido = await uploadJsonToDrive(
      archivoAsistencias,
      "Archivos de Respaldo",
      nombreArchivo
    );
    console.log(`   ✅ Archivo subido exitosamente`);
    console.log(`   🆔 ID del archivo: ${archivoSubido.id}`);

    // 5. Gestionar archivo de IDs
    console.log("\n🗂️ PASO 4: Gestionando archivo de IDs...");
    const fechaISO = formatearFechaParaArchivoIds(fechaActual);
    const resultadoGestion = await gestionarArchivoIdsArchivosUltimasAsistenciasEscolaresDiarias(
      nivel,
      fechaISO,
      archivoSubido.id,
      config.limiteArchivos
    );

    console.log("\n" + "=".repeat(70));
    console.log("✅ PROCESO COMPLETADO EXITOSAMENTE");
    console.log("=".repeat(70));
    console.log(`📦 Archivo creado: ${nombreArchivo}`);
    console.log(`🆔 ID: ${archivoSubido.id}`);
    console.log(
      `🗑️ Archivos eliminados: ${resultadoGestion.archivosEliminados.length}`
    );
    console.log("=".repeat(70) + "\n");

    return {
      archivoCreado: {
        id: archivoSubido.id,
        nombre: nombreArchivo,
      },
      archivosEliminados: resultadoGestion.archivosEliminados,
    };
  } catch (error) {
    console.error("\n❌ ERROR EN PROCESAMIENTO DE ASISTENCIAS DIARIAS:", error);
    throw error;
  }
}
