import {
  T_Archivos_Respaldo_Google_Drive,
  T_Aulas,
  T_Estudiantes,
  T_Modificaciones_Especificas,
} from "@prisma/client";
import {
  GradosPrimaria,
  GradosSecundaria,
} from "../../constants/GRADOS_POR_NIVEL_EDUCATIVO";
import { closeClient } from "../../core/databases/connectors/mongodb";
import { closePool } from "../../core/databases/connectors/postgres";
import { obtenerArchivosRespaldoDeUltimasListasEstudiantes } from "../../core/databases/queries/RDP02/archivos-respaldo/obtenerArchivosListasEstudiantes";
import { obtenerAulasPorGradoYNivel } from "../../core/databases/queries/RDP03/aulas/obtenerAulasPorGradoYNivel";
import {
  GradosPorNivel,
  obtenerEstudiantesPorGradoYNivel,
} from "../../core/databases/queries/RDP03/estudiantes/obtenerEstudiantesPorNivelYGrado";
import { obtenerModificacionesEspecificasEstudiantes } from "../../core/databases/queries/RDP03/modificaciones-especificas/obtenerModificacionesEspecificasDeEstudiantes";
import { actualizarArchivoRespaldoEnGoogleDrive } from "../../core/external/google/drive/actualizarArchivoDatosAsistencia";
import { descargarArchivoJSONDesdeGoogleDrive } from "../../core/external/google/drive/descargarArchivoJSONDesdeGoogle";
import { guardarObjetoComoJSONEnBlobs } from "../../core/external/vercel/blobs/guardarObjetoComoJSONEnBlobs";
import { ListaEstudiantesPorGradoParaHoy } from "../../interfaces/shared/Asistencia/ListaEstudiantesPorGradosParaHoy";
import { NivelEducativo } from "../../interfaces/shared/NivelEducativo";
import { generarNombreArchivo } from "../../core/utils/helpers/generators/generarNombreArchivoJSONListaEstudiantes";
import { obtenerFechasActuales } from "../../core/utils/dates/obtenerFechasActuales";
import verificarFueraAñoEscolar from "../../core/databases/queries/RDP02/fechas-importantes/verificarDentroAñoEscolar";
import { obtenerFechasAñoEscolar } from "../../core/databases/queries/RDP02/fechas-importantes/obtenerFechasAñoEscolar";
import { NOMBRE_ARCHIVO_REPORTE_ACTUALIZACION_DE_LISTAS_DE_ESTUDIANTES } from "../../constants/NOMBRE_ARCHIVOS_SISTEMA";
import { ReporteActualizacionDeListasEstudiantes } from "../../interfaces/shared/Asistencia/ReporteModificacionesListasDeEstudiantes";
import { registrarFaltasAutomaticasEstudiantesInactivos } from "../../core/databases/queries/RDP03/asistencias-escolares/registrarAsistenciaAutoNullParaEstudiantesInactivos";
/**
 * Inicializa el reporte con todos los archivos de estudiantes y fechas por defecto
 */
function inicializarReporteActualizacion(
  fechaActual: Date
): ReporteActualizacionDeListasEstudiantes {
  // Crear objeto con todas las propiedades requeridas por el tipo
  const estadoInicial = {} as Record<string, Date>;

  // Agregar todos los archivos de PRIMARIA
  for (const grado of Object.values(GradosPrimaria)) {
    if (typeof grado === "number") {
      const nombreArchivo = generarNombreArchivo(
        NivelEducativo.PRIMARIA,
        grado as GradosPrimaria
      );
      estadoInicial[nombreArchivo] = fechaActual; // Fecha por defecto
    }
  }

  // Agregar todos los archivos de SECUNDARIA
  for (const grado of Object.values(GradosSecundaria)) {
    if (typeof grado === "number") {
      const nombreArchivo = generarNombreArchivo(
        NivelEducativo.SECUNDARIA,
        grado as GradosSecundaria
      );
      estadoInicial[nombreArchivo] = fechaActual; // Fecha por defecto
    }
  }

  return {
    EstadoDeListasDeEstudiantes: estadoInicial as any, // Cast necesario para satisfacer TypeScript
    Fecha_Actualizacion: fechaActual,
  };
}

/**
 * Verifica si hay modificaciones para una combinación específica de nivel y grado
 */
function buscarModificacionParaNivelYGrado<T extends NivelEducativo>(
  modificaciones: T_Modificaciones_Especificas[],
  nivel: T,
  grado: T extends NivelEducativo.PRIMARIA ? GradosPrimaria : GradosSecundaria
): T_Modificaciones_Especificas | undefined {
  return modificaciones.find(
    (m) => m.Valores_Campos_Identificadores === `${nivel},${grado}`
  );
}

/**
 * Busca un archivo existente para una combinación de nivel y grado
 */
function buscarArchivoExistente<T extends NivelEducativo>(
  archivos: T_Archivos_Respaldo_Google_Drive[],
  nivel: T,
  grado: T extends NivelEducativo.PRIMARIA ? GradosPrimaria : GradosSecundaria
): T_Archivos_Respaldo_Google_Drive | undefined {
  const nombreArchivo = generarNombreArchivo(nivel, grado);
  return archivos.find((archivo) => archivo.Nombre_Archivo === nombreArchivo);
}

/**
 * Procesa una combinación específica de nivel y grado
 */
async function procesarNivelYGrado<T extends NivelEducativo>(
  nivel: T,
  grado: T extends NivelEducativo.PRIMARIA ? GradosPrimaria : GradosSecundaria,
  modificaciones: T_Modificaciones_Especificas[],
  archivosExistentes: T_Archivos_Respaldo_Google_Drive[],
  estadoReporte: Record<string, Date>
): Promise<boolean> {
  try {
    console.log(`\n🔄 Procesando ${nivel} - Grado ${grado}`);

    const { fechaUTC, fechaLocalPeru } = obtenerFechasActuales();
    const nombreArchivo = generarNombreArchivo(nivel, grado);

    // Verificar si hay modificaciones para esta combinación
    const modificacion = buscarModificacionParaNivelYGrado(
      modificaciones,
      nivel,
      grado
    );

    // Buscar archivo existente
    const archivoExistente = buscarArchivoExistente(
      archivosExistentes,
      nivel,
      grado
    );

    let debeActualizar = false;
    let fechaParaReporte = fechaUTC; // Por defecto, usar fecha actual
    let estudiantes: T_Estudiantes[] = [];
    let aulas: T_Aulas[] = [];

    if (!modificacion) {
      // No hay modificaciones, consultar desde cero por seguridad
      console.log(
        `⚠️ No hay modificaciones registradas para ${nivel} grado ${grado}, consultando desde cero`
      );
      debeActualizar = true;

      estudiantes = await obtenerEstudiantesPorGradoYNivel(
        nivel,
        grado as GradosPorNivel<typeof nivel>
      );
      aulas = await obtenerAulasPorGradoYNivel(nivel, grado);
    } else {
      console.log(`✅ Encontrada modificación para ${nivel} grado ${grado}`);

      if (!archivoExistente) {
        // No existe archivo previo, crear desde cero
        console.log(
          `📝 No existe archivo previo para ${nivel} grado ${grado}, creando desde cero`
        );
        debeActualizar = true;

        estudiantes = await obtenerEstudiantesPorGradoYNivel(
          nivel,
          grado as GradosPorNivel<typeof nivel>
        );
        aulas = await obtenerAulasPorGradoYNivel(nivel, grado);
      } else {
        // Descargar archivo existente y comparar fechas
        try {
          console.log(
            `📥 Descargando archivo existente para comparar fechas...`
          );
          const datosExistentes = await descargarArchivoJSONDesdeGoogleDrive<
            ListaEstudiantesPorGradoParaHoy<T>
          >(archivoExistente.Google_Drive_Id);

          const fechaModificacion = new Date(modificacion.Fecha_Modificacion);
          const fechaArchivoExistente = new Date(
            datosExistentes.Fecha_Actualizacion
          );

          if (fechaModificacion > fechaArchivoExistente) {
            console.log(
              `🔄 Modificación más reciente que archivo existente, actualizando...`
            );
            debeActualizar = true;

            estudiantes = await obtenerEstudiantesPorGradoYNivel(
              nivel,
              grado as GradosPorNivel<typeof nivel>
            );
            aulas = await obtenerAulasPorGradoYNivel(nivel, grado);
          } else {
            console.log(
              `✅ Archivo existente está actualizado, no se requiere actualización`
            );
            debeActualizar = false;

            // Usar la fecha del archivo existente para el reporte
            fechaParaReporte = fechaArchivoExistente;
          }
        } catch (downloadError) {
          console.error(
            `❌ Error al descargar archivo existente, consultando desde cero:`,
            downloadError
          );
          debeActualizar = true;

          estudiantes = await obtenerEstudiantesPorGradoYNivel(
            nivel,
            grado as GradosPorNivel<typeof nivel>
          );
          aulas = await obtenerAulasPorGradoYNivel(nivel, grado);
        }
      }
    }

    if (debeActualizar) {
      // Construir objeto final
      const listaFinal: ListaEstudiantesPorGradoParaHoy<T> = {
        ListaEstudiantes: estudiantes,
        Aulas: aulas,
        Nivel: nivel,
        Grado: grado,
        Fecha_Actualizacion: fechaUTC,
        Fecha_Actualizacion_Peru: fechaLocalPeru,
      };

      console.log(
        `💾 Guardando archivo ${nombreArchivo} con ${estudiantes.length} estudiantes y ${aulas.length} aulas`
      );

      // Guardar en Vercel Blobs
      await guardarObjetoComoJSONEnBlobs(listaFinal, nombreArchivo);

      // Actualizar archivo de respaldo en Google Drive
      await actualizarArchivoRespaldoEnGoogleDrive(nombreArchivo, listaFinal);

      console.log(`✅ ${nombreArchivo} actualizado correctamente`);

      // Actualizar fecha en el reporte con la nueva fecha
      estadoReporte[nombreArchivo] = fechaUTC;
    } else {
      console.log(`⏭️ ${nombreArchivo} no requiere actualización`);

      // Actualizar fecha en el reporte con la fecha existente
      estadoReporte[nombreArchivo] = fechaParaReporte;
    }

    return debeActualizar;
  } catch (error) {
    console.error(`❌ Error procesando ${nivel} grado ${grado}:`, error);
    throw error;
  }
}

/**
 * Función principal que maneja todo el proceso de actualización
 */
async function main() {
  try {
    // Obtener fechas actuales
    const { fechaUTC, fechaLocalPeru } = obtenerFechasActuales();

    // Verificar si estamos dentro del año escolar
    const fechasAñoEscolar = await obtenerFechasAñoEscolar();
    const fueraAñoEscolar = verificarFueraAñoEscolar(
      fechaLocalPeru,
      fechasAñoEscolar.Inicio_Año_Escolar,
      fechasAñoEscolar.Fin_Año_Escolar
    );

    if (fueraAñoEscolar) {
      console.log(
        "🚫 Fuera del año escolar, no se procesará la actualización de registros de listas de estudiantes."
      );
      return;
    }

    console.log(
      "🚀 Iniciando sistema de actualización de listas de estudiantes..."
    );

    // Inicializar reporte con todas las propiedades requeridas
    console.log("\n📊 Inicializando reporte de actualización...");
    const reporteActualizacion = inicializarReporteActualizacion(fechaUTC);

    console.log(
      `📋 Reporte inicializado con ${
        Object.keys(reporteActualizacion.EstadoDeListasDeEstudiantes).length
      } archivos`
    );

    // 1. Obtener modificaciones específicas de estudiantes
    console.log("\n📋 Paso 1: Obteniendo modificaciones específicas...");
    const modificaciones = await obtenerModificacionesEspecificasEstudiantes();

    // 2. Obtener archivos existentes de estudiantes
    console.log("\n📁 Paso 2: Obteniendo archivos existentes...");
    const archivosExistentes =
      await obtenerArchivosRespaldoDeUltimasListasEstudiantes();

    // 3. Procesar cada nivel y grado
    console.log("\n🔄 Paso 3: Procesando cada nivel y grado...");

    let archivosActualizados = 0;
    let archivosNoActualizados = 0;

    // Iterar por PRIMARIA
    console.log("\n📚 === PROCESANDO PRIMARIA ===");
    for (const grado of Object.values(GradosPrimaria)) {
      if (typeof grado === "number") {
        const fueActualizado = await procesarNivelYGrado(
          NivelEducativo.PRIMARIA,
          grado as GradosPrimaria,
          modificaciones,
          archivosExistentes,
          reporteActualizacion.EstadoDeListasDeEstudiantes
        );

        if (fueActualizado) {
          archivosActualizados++;
        } else {
          archivosNoActualizados++;
        }
      }
    }

    // Iterar por SECUNDARIA
    console.log("\n🎓 === PROCESANDO SECUNDARIA ===");
    for (const grado of Object.values(GradosSecundaria)) {
      if (typeof grado === "number") {
        const fueActualizado = await procesarNivelYGrado(
          NivelEducativo.SECUNDARIA,
          grado as GradosSecundaria,
          modificaciones,
          archivosExistentes,
          reporteActualizacion.EstadoDeListasDeEstudiantes
        );

        if (fueActualizado) {
          archivosActualizados++;
        } else {
          archivosNoActualizados++;
        }
      }
    }

    // 4. Finalizar reporte y guardarlo
    console.log(
      "\n📊 Paso 4: Finalizando y guardando reporte de actualizaciones..."
    );

    // Actualizar fecha final del reporte
    reporteActualizacion.Fecha_Actualizacion = fechaUTC;

    console.log(
      `💾 Guardando reporte de actualización con ${
        Object.keys(reporteActualizacion.EstadoDeListasDeEstudiantes).length
      } archivos registrados`
    );

    console.log(
      `📊 Resumen de procesamiento: ${archivosActualizados} actualizados, ${archivosNoActualizados} sin cambios`
    );

    // Guardar reporte en Vercel Blobs
    await guardarObjetoComoJSONEnBlobs(
      reporteActualizacion,
      NOMBRE_ARCHIVO_REPORTE_ACTUALIZACION_DE_LISTAS_DE_ESTUDIANTES
    );

    // Guardar reporte en Google Drive
    await actualizarArchivoRespaldoEnGoogleDrive(
      NOMBRE_ARCHIVO_REPORTE_ACTUALIZACION_DE_LISTAS_DE_ESTUDIANTES,
      reporteActualizacion
    );

    console.log(`✅ Reporte de actualización guardado correctamente`);

    // Mostrar resumen del reporte
    console.log("\n📋 === RESUMEN DEL REPORTE ===");
    Object.entries(reporteActualizacion.EstadoDeListasDeEstudiantes).forEach(
      ([archivo, fecha]) => {
        console.log(`📄 ${archivo}: ${fecha.toISOString()}`);
      }
    );

    console.log(
      "\n✅ Sistema de actualización de listas de estudiantes completado exitosamente"
    );

    // NUEVA FUNCIONALIDAD: Registro de faltas automáticas para estudiantes inactivos
    console.log(
      "\n🔄 Iniciando registro de faltas automáticas para estudiantes inactivos..."
    );

    try {
      const resultadoFaltasInactivos =
        await registrarFaltasAutomaticasEstudiantesInactivos();

      console.log(
        "✅ Registro de faltas automáticas para estudiantes inactivos completado:"
      );
      console.log(
        `- Total estudiantes inactivos procesados: ${resultadoFaltasInactivos.totalEstudiantesInactivos}`
      );
      console.log(
        `- Faltas completas registradas: ${resultadoFaltasInactivos.faltasCompletasRegistradas}`
      );
      console.log(`- Errores encontrados: ${resultadoFaltasInactivos.errores}`);
    } catch (faltasError) {
      // No interrumpimos el proceso principal por un error en el registro de faltas
      console.error(
        "❌ Error al procesar faltas automáticas para estudiantes inactivos, pero continuando:",
        faltasError
      );
    }
  } catch (error) {
    console.error(
      "❌ Error en el sistema de actualización de listas de estudiantes:",
      error
    );
    process.exit(1);
  } finally {
    // Cerrar todas las conexiones
    console.log("\n🔌 Cerrando conexiones...");
    await Promise.all([closePool(), closeClient()]);
    console.log("✅ Conexiones cerradas. Finalizando proceso...");
    process.exit(0);
  }
}

// Ejecutar el script
main();
