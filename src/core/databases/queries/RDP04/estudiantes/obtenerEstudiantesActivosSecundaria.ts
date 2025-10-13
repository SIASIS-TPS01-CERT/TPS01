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
 * 🆕 ACTUALIZADO: Ahora incluye información completa del aula y sección
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

        // 🆕 Crear un mapa de aulas por Id_Aula para acceso rápido
        const mapaAulas = new Map(
          datosLista.Aulas.map((aula) => [aula.Id_Aula, aula])
        );

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
          // 🆕 Obtener información del aula del estudiante
          const aulaEstudiante = mapaAulas.get(estudiante.Id_Aula!);

          if (!aulaEstudiante) {
            console.warn(
              `⚠️ No se encontró información del aula para estudiante ${estudiante.Id_Estudiante} (Id_Aula: ${estudiante.Id_Aula})`
            );
            continue;
          }

          estudiantesActivos.push({
            idEstudiante: estudiante.Id_Estudiante,
            nombres: estudiante.Nombres,
            apellidos: estudiante.Apellidos,
            grado: grado,
            seccion: aulaEstudiante.Seccion, // 🆕 AGREGADO
            nivel: NivelEducativo.SECUNDARIA,
            tablaAsistencia: tablaAsistencia,
            nombreCompleto: `${estudiante.Nombres} ${estudiante.Apellidos}`,
            // 🆕 NUEVO: Información completa del aula
            aula: {
              Nivel: aulaEstudiante.Nivel,
              Grado: aulaEstudiante.Grado,
              Seccion: aulaEstudiante.Seccion,
              Color: aulaEstudiante.Color,
              Id_Profesor_Primaria: aulaEstudiante.Id_Profesor_Primaria,
              Id_Profesor_Secundaria: aulaEstudiante.Id_Profesor_Secundaria,
              Id_Aula: Number(aulaEstudiante.Id_Aula),
            },
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

    // Mostrar resumen por grado y sección
    const resumenPorGradoSeccion = estudiantesActivos.reduce(
      (acc, estudiante) => {
        const clave = `${estudiante.grado}${estudiante.seccion}`;
        if (!acc[clave]) {
          acc[clave] = {
            grado: estudiante.grado,
            seccion: estudiante.seccion,
            cantidad: 0,
          };
        }
        acc[clave].cantidad++;
        return acc;
      },
      {} as Record<string, { grado: number; seccion: string; cantidad: number }>
    );

    console.log("📊 Resumen por grado y sección:");
    Object.values(resumenPorGradoSeccion)
      .sort((a, b) => {
        if (a.grado !== b.grado) return a.grado - b.grado;
        return a.seccion.localeCompare(b.seccion);
      })
      .forEach(({ grado, seccion, cantidad }) => {
        console.log(`   Grado ${grado}${seccion}: ${cantidad} estudiantes`);
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
