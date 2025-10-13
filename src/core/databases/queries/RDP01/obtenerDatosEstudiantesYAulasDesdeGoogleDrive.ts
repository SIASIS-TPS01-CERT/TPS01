import { NivelEducativo } from "../../../../interfaces/shared/NivelEducativo";
import { descargarArchivoJSONDesdeGoogleDrive } from "../../../external/google/drive/descargarArchivoJSONDesdeGoogle";
import { obtenerArchivosRespaldoDeUltimasListasEstudiantes } from "../../../databases/queries/RDP02/archivos-respaldo/obtenerArchivosListasEstudiantes";

/**
 * Interfaz para estudiante desde el archivo JSON
 */
export interface EstudianteDesdeJSON {
  Nombres: string;
  Apellidos: string;
  Estado: boolean;
  Google_Drive_Foto_ID: string | null;
  Id_Aula: string;
  Id_Estudiante: string;
}

/**
 * Interfaz para aula desde el archivo JSON
 */
export interface AulaDesdeJSON {
  Nivel: string;
  Grado: number;
  Seccion: string;
  Color: string;
  Id_Profesor_Primaria: string | null;
  Id_Profesor_Secundaria: string | null;
  Id_Aula: string;
}

/**
 * Interfaz para el archivo JSON completo
 */
interface ArchivoListaEstudiantes {
  ListaEstudiantes: EstudianteDesdeJSON[];
  Aulas: AulaDesdeJSON[];
  Nivel: string;
  Grado: number;
  Fecha_Actualizacion: string;
  Fecha_Actualizacion_Peru: string;
}

/**
 * Interfaz para los datos consolidados
 */
export interface DatosConsolidados {
  estudiantes: Map<string, EstudianteDesdeJSON>;
  aulas: Map<string, AulaDesdeJSON>;
}

/**
 * Obtiene y consolida todos los datos de estudiantes y aulas desde Google Drive
 * @param nivel - Nivel educativo (PRIMARIA o SECUNDARIA)
 * @returns Mapas consolidados de estudiantes y aulas
 */
export async function obtenerDatosEstudiantesYAulasDesdeGoogleDrive(
  nivel: NivelEducativo
): Promise<DatosConsolidados> {
  try {
    const nivelTexto =
      nivel === NivelEducativo.SECUNDARIA ? "SECUNDARIA" : "PRIMARIA";
    const prefijoNivel = nivel === NivelEducativo.SECUNDARIA ? "S" : "P";

    console.log(
      `   📥 Obteniendo datos de estudiantes y aulas de ${nivelTexto} desde Google Drive...`
    );

    // 1. Obtener archivos de respaldo de listas de estudiantes
    const archivosRespaldo =
      await obtenerArchivosRespaldoDeUltimasListasEstudiantes();

    // 2. Filtrar solo archivos del nivel correspondiente
    const archivosNivel = archivosRespaldo.filter((archivo) =>
      archivo.Nombre_Archivo.includes(`Estudiantes_${prefijoNivel}_`)
    );

    console.log(
      `   📁 Encontrados ${archivosNivel.length} archivos de listas de estudiantes de ${nivelTexto}`
    );

    if (archivosNivel.length === 0) {
      console.warn(
        `   ⚠️  No se encontraron archivos de listas de estudiantes para ${nivelTexto}`
      );
      return {
        estudiantes: new Map(),
        aulas: new Map(),
      };
    }

    // 3. Mapas para consolidar datos
    const estudiantesMap = new Map<string, EstudianteDesdeJSON>();
    const aulasMap = new Map<string, AulaDesdeJSON>();

    // 4. Descargar y procesar cada archivo
    for (const archivo of archivosNivel) {
      try {
        console.log(`   📥 Descargando ${archivo.Nombre_Archivo}...`);

        const datosArchivo =
          await descargarArchivoJSONDesdeGoogleDrive<ArchivoListaEstudiantes>(
            archivo.Google_Drive_Id
          );

        // Agregar estudiantes al mapa
        for (const estudiante of datosArchivo.ListaEstudiantes) {
          estudiantesMap.set(estudiante.Id_Estudiante, estudiante);
        }

        // Agregar aulas al mapa
        for (const aula of datosArchivo.Aulas) {
          aulasMap.set(aula.Id_Aula, aula);
        }

        console.log(
          `      ✅ Grado ${datosArchivo.Grado}: ${datosArchivo.ListaEstudiantes.length} estudiantes, ${datosArchivo.Aulas.length} aulas`
        );
      } catch (error) {
        console.error(
          `   ❌ Error descargando archivo ${archivo.Nombre_Archivo}:`,
          error
        );
      }
    }

    console.log(
      `   ✅ Datos consolidados: ${estudiantesMap.size} estudiantes, ${aulasMap.size} aulas`
    );

    return {
      estudiantes: estudiantesMap,
      aulas: aulasMap,
    };
  } catch (error) {
    console.error("❌ Error obteniendo datos desde Google Drive:", error);
    return {
      estudiantes: new Map(),
      aulas: new Map(),
    };
  }
}
