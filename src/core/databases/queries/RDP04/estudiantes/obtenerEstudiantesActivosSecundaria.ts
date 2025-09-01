import { GradosSecundaria } from "../../../../../constants/GRADOS_POR_NIVEL_EDUCATIVO";
import { NOMBRES_ARCHIVOS_LISTAS_ESTUDIANTES_DIARIAS } from "../../../../../constants/NOMBRE_ARCHIVOS_SISTEMA";
import { ListaEstudiantesPorGradoParaHoy } from "../../../../../interfaces/shared/Asistencia/ListaEstudiantesPorGradosParaHoy";
import { NivelEducativo } from "../../../../../interfaces/shared/NivelEducativo";
import { TABLAS_ASISTENCIAS_ESCOLARES } from "../../../../../interfaces/shared/RDP03/TablasDeAsistenciaEscolar";
import { EstudianteActivoSecundaria } from "../../../../../jobs/asistenciaEscolar/SetAsistenciasYFaltasEstudiantesSecundaria";
import { descargarArchivoJSONDesdeGoogleDrive } from "../../../../external/google/drive/descargarArchivoJSONDesdeGoogle";
import { obtenerArchivosRespaldoDeUltimasListasEstudiantes } from "../../RDP02/archivos-respaldo/obtenerArchivosListasEstudiantes";

/**
 * Obtiene todos los estudiantes activos de secundaria desde las listas de Google Drive
 */
export async function obtenerEstudiantesActivosSecundaria(): Promise<
  EstudianteActivoSecundaria[]
> {
  try {
    console.log(
      "📋 Obteniendo estudiantes activos de secundaria desde Google Drive..."
    );

    // 1. Obtener archivos de respaldo de listas de estudiantes
    const archivosRespaldo =
      await obtenerArchivosRespaldoDeUltimasListasEstudiantes();

    // 2. Filtrar solo archivos de secundaria
    const archivosSecundaria = archivosRespaldo.filter((archivo) =>
      archivo.Nombre_Archivo.includes("Estudiantes_S_")
    );

    console.log(
      `📁 Encontrados ${archivosSecundaria.length} archivos de listas de estudiantes de secundaria`
    );

    if (archivosSecundaria.length === 0) {
      console.warn(
        "⚠️ No se encontraron archivos de listas de estudiantes de secundaria"
      );
      return [];
    }

    const estudiantesActivos: EstudianteActivoSecundaria[] = [];

    // 3. Procesar cada grado de secundaria
    for (const grado of Object.values(GradosSecundaria)) {
      if (typeof grado !== "number") continue;

      try {
        // Buscar archivo para este grado
        const nombreArchivoEsperado =
          NOMBRES_ARCHIVOS_LISTAS_ESTUDIANTES_DIARIAS[
            NivelEducativo.SECUNDARIA
          ][grado];
        const archivo = archivosSecundaria.find((a) =>
          a.Nombre_Archivo.startsWith(nombreArchivoEsperado)
        );

        if (!archivo) {
          console.warn(
            `⚠️ No se encontró archivo para secundaria grado ${grado}: ${nombreArchivoEsperado}`
          );
          continue;
        }

        console.log(
          `📥 Descargando lista de estudiantes para secundaria grado ${grado}...`
        );

        // Descargar y procesar archivo
        const datosLista = await descargarArchivoJSONDesdeGoogleDrive<
          ListaEstudiantesPorGradoParaHoy<NivelEducativo.SECUNDARIA>
        >(archivo.Google_Drive_Id);

        // Filtrar solo estudiantes activos
        const estudiantesGrado = datosLista.ListaEstudiantes.filter(
          (estudiante) => estudiante.Estado
        );

        console.log(
          `👥 Grado ${grado}: ${estudiantesGrado.length} estudiantes activos encontrados`
        );

        // Obtener tabla de asistencia correspondiente
        const tablaAsistencia =
          TABLAS_ASISTENCIAS_ESCOLARES[NivelEducativo.SECUNDARIA][grado];

        // Agregar estudiantes a la lista
        for (const estudiante of estudiantesGrado) {
          estudiantesActivos.push({
            idEstudiante: estudiante.Id_Estudiante,
            nombres: estudiante.Nombres,
            apellidos: estudiante.Apellidos,
            grado: grado,
            nivel: "S",
            tablaAsistencia: tablaAsistencia,
            nombreCompleto: `${estudiante.Nombres} ${estudiante.Apellidos}`,
          });
        }
      } catch (error) {
        console.error(
          `❌ Error procesando grado ${grado} de secundaria:`,
          error
        );
      }
    }

    console.log(
      `✅ Total estudiantes activos de secundaria obtenidos: ${estudiantesActivos.length}`
    );

    // Mostrar resumen por grado
    const resumenPorGrado = estudiantesActivos.reduce((acc, estudiante) => {
      acc[estudiante.grado] = (acc[estudiante.grado] || 0) + 1;
      return acc;
    }, {} as Record<number, number>);

    console.log("📊 Resumen por grado:");
    Object.entries(resumenPorGrado).forEach(([grado, cantidad]) => {
      console.log(`   Grado ${grado}: ${cantidad} estudiantes`);
    });

    return estudiantesActivos;
  } catch (error) {
    console.error(
      "❌ Error obteniendo estudiantes activos de secundaria:",
      error
    );
    return [];
  }
}
