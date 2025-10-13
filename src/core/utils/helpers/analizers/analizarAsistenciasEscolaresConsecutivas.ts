import { ArchivoAsistenciaEscolarDiaria } from "../../../../interfaces/shared/Asistencia/ArchivoAsistenciaEscolarDiaria";
import { NivelEducativo } from "../../../../interfaces/shared/NivelEducativo";
import { obtenerDatosEstudiantesYAulasDesdeGoogleDrive } from "../../../databases/queries/RDP01/obtenerDatosEstudiantesYAulasDesdeGoogleDrive";
import { ConfiguracionesReportes } from "../../../databases/queries/RDP02/ajustes-generales/obtenerConfiguracionesReportesEscolares";
import { descargarArchivoJSONDesdeGoogleDrive } from "../../../external/google/drive/descargarArchivoJSONDesdeGoogle";
import { ArchivoAsistenciaEscolarReciente } from "../verificators/verificarArchivosAsistenciaEscolarDisponibles";



export interface EstudianteConProblema {
  idEstudiante: string;
  nombres: string;
  apellidos: string;
  grado: number;
  seccion: string;
  colorAula: string;
  diasConsecutivos: DiaAsistencia[];
}

export interface DiaAsistencia {
  fecha: string;
  estado: "falta" | "tardanza";
  horaLlegada?: string; // Solo para tardanzas
  desfaseSegundos?: number; // Solo para tardanzas
}

export interface ResultadosAnalisis {
  estudiantesConFaltas: EstudianteConProblema[];
  estudiantesConTardanzas: EstudianteConProblema[];
  aulasAfectadas: Set<string>; // IDs de aulas con problemas
}

export async function analizarAsistenciasEscolaresConsecutivas(
  nivel: NivelEducativo,
  archivos: ArchivoAsistenciaEscolarReciente[],
  configuraciones: ConfiguracionesReportes,
  analizarFaltas: boolean,
  analizarTardanzas: boolean
): Promise<ResultadosAnalisis> {
  try {
    console.log("   🔄 Descargando y procesando archivos de asistencia...");

    // Tomar solo los archivos necesarios
    const cantidadArchivos = Math.max(
      analizarFaltas ? configuraciones.faltasConsecutivasMaximas : 0,
      analizarTardanzas ? configuraciones.tardanzasConsecutivasMaximas : 0
    );

    const archivosAProcesar = archivos.slice(0, cantidadArchivos);

    // Descargar todos los archivos
    const datosArchivos = await Promise.all(
      archivosAProcesar.map(async (archivo) => {
        const data =
          await descargarArchivoJSONDesdeGoogleDrive<ArchivoAsistenciaEscolarDiaria>(
            archivo.googleDriveId
          );

        return {
          fecha: archivo.fechaISO8601,
          data: data,
        };
      })
    );

    // Ordenar por fecha (más antigua primero para análisis consecutivo)
    datosArchivos.sort(
      (a, b) => new Date(a.fecha).getTime() - new Date(b.fecha).getTime()
    );

    console.log(
      `   ✅ ${datosArchivos.length} archivos descargados y ordenados`
    );

    console.log(`   📋 Procesando archivos para detectar consecutivos...`);

    // Mapas para rastrear consecutivos por estudiante
    const faltasConsecutivasPorEstudiante = new Map<string, DiaAsistencia[]>();
    const tardanzasConsecutivasPorEstudiante = new Map<
      string,
      DiaAsistencia[]
    >();

    let totalFaltasProcesadas = 0;
    let totalTardanzasProcesadas = 0;

    // Procesar cada archivo (del más antiguo al más reciente)
    for (const archivo of datosArchivos) {
      // Iterar por grados
      for (const grado in archivo.data) {
        const secciones = archivo.data[grado];

        // Iterar por secciones
        for (const seccion in secciones) {
          const { ListaAsistenciasEscolares } = secciones[seccion];

          // Iterar por estudiantes
          for (const idEstudiante in ListaAsistenciasEscolares) {
            const asistencia = ListaAsistenciasEscolares[idEstudiante];

            // Obtener el registro de entrada (key "E")
            const entrada = asistencia["E"];

            // CASO 1: El estudiante NO VINO (falta)
            // Si DesfaseSegundos es null, significa que no asistió
            if (!entrada || entrada.DesfaseSegundos === null) {
              // Acumular falta si estamos analizando faltas
              if (analizarFaltas) {
                const faltas =
                  faltasConsecutivasPorEstudiante.get(idEstudiante) || [];
                faltas.push({
                  fecha: archivo.fecha,
                  estado: "falta",
                });
                faltasConsecutivasPorEstudiante.set(idEstudiante, faltas);
                totalFaltasProcesadas++;
              }

              // Si faltó, resetear tardanzas porque no puede tener tardanza si no vino
              if (analizarTardanzas) {
                tardanzasConsecutivasPorEstudiante.delete(idEstudiante);
              }
              continue;
            }

            const desfaseSegundos = entrada.DesfaseSegundos;

            // CASO 2: El estudiante VINO A TIEMPO o ANTES
            // (DesfaseSegundos <= 0: llegó exactamente a tiempo o antes)
            if (desfaseSegundos !== null && desfaseSegundos <= 0) {
              // Si llegó a tiempo, rompe la racha de faltas
              if (analizarFaltas) {
                faltasConsecutivasPorEstudiante.delete(idEstudiante);
              }

              // Si llegó a tiempo, rompe la racha de tardanzas
              if (analizarTardanzas) {
                tardanzasConsecutivasPorEstudiante.delete(idEstudiante);
              }
              continue;
            }

            // CASO 3: El estudiante LLEGÓ TARDE (tiene desfase positivo)
            // Como llegó (aunque tarde), rompe la racha de faltas
            if (analizarFaltas) {
              faltasConsecutivasPorEstudiante.delete(idEstudiante);
            }

            // Verificar si la tardanza supera la tolerancia
            if (
              analizarTardanzas &&
              desfaseSegundos !== null &&
              desfaseSegundos > 0
            ) {
              const toleranciaSegundos =
                configuraciones.toleranciaTardanzaMinutos * 60;

              if (desfaseSegundos > toleranciaSegundos) {
                // Acumular tardanza
                const tardanzas =
                  tardanzasConsecutivasPorEstudiante.get(idEstudiante) || [];

                // Calcular hora de llegada exacta
                const horaBase = new Date(
                  `2000-01-01T${configuraciones.horaInicioClases}`
                );
                const horaLlegada = new Date(
                  horaBase.getTime() + desfaseSegundos * 1000
                );
                const horaLlegadaStr = horaLlegada.toTimeString().split(" ")[0];

                tardanzas.push({
                  fecha: archivo.fecha,
                  estado: "tardanza",
                  horaLlegada: horaLlegadaStr,
                  desfaseSegundos: desfaseSegundos - toleranciaSegundos,
                });
                tardanzasConsecutivasPorEstudiante.set(idEstudiante, tardanzas);
                totalTardanzasProcesadas++;
              } else {
                // Llegó tarde pero dentro de la tolerancia (se considera puntual)
                // Esto rompe la racha de tardanzas
                tardanzasConsecutivasPorEstudiante.delete(idEstudiante);
              }
            }
          }
        }
      }
    }

    console.log(`   📊 Procesamiento completado:`);
    console.log(
      `      - Total de faltas registradas: ${totalFaltasProcesadas}`
    );
    console.log(
      `      - Total de tardanzas registradas: ${totalTardanzasProcesadas}`
    );
    console.log(
      `      - Estudiantes con al menos 1 falta: ${faltasConsecutivasPorEstudiante.size}`
    );
    console.log(
      `      - Estudiantes con al menos 1 tardanza: ${tardanzasConsecutivasPorEstudiante.size}`
    );

    console.log(
      `   🔍 Identificando estudiantes con problemas consecutivos...`
    );

    // Filtrar estudiantes que alcanzaron el umbral
    const estudiantesConFaltasIds: string[] = [];
    const estudiantesConTardanzasIds: string[] = [];

    if (analizarFaltas) {
      for (const [idEstudiante, faltas] of faltasConsecutivasPorEstudiante) {
        if (faltas.length >= configuraciones.faltasConsecutivasMaximas) {
          estudiantesConFaltasIds.push(idEstudiante);
        }
      }
      console.log(
        `   ✓ Umbral de faltas: ${configuraciones.faltasConsecutivasMaximas} días consecutivos`
      );
    }

    if (analizarTardanzas) {
      for (const [
        idEstudiante,
        tardanzas,
      ] of tardanzasConsecutivasPorEstudiante) {
        if (tardanzas.length >= configuraciones.tardanzasConsecutivasMaximas) {
          estudiantesConTardanzasIds.push(idEstudiante);
        }
      }
      console.log(
        `   ✓ Umbral de tardanzas: ${configuraciones.tardanzasConsecutivasMaximas} días consecutivos`
      );
    }

    console.log(`   📊 Estudiantes identificados:`);
    console.log(`      - Con faltas: ${estudiantesConFaltasIds.length}`);
    console.log(`      - Con tardanzas: ${estudiantesConTardanzasIds.length}`);

    // Debug: Mostrar algunos IDs de ejemplo
    if (estudiantesConFaltasIds.length > 0) {
      console.log(`   🔎 Ejemplos de IDs con faltas:`);
      console.log(`      ${estudiantesConFaltasIds.slice(0, 5).join(", ")}`);
    }
    if (estudiantesConTardanzasIds.length > 0) {
      console.log(`   🔎 Ejemplos de IDs con tardanzas:`);
      console.log(`      ${estudiantesConTardanzasIds.slice(0, 5).join(", ")}`);
    }

    // ============================================================
    // 🆕 NUEVA LÓGICA: Obtener datos desde Google Drive
    // ============================================================
    const todosLosIds = [
      ...new Set([...estudiantesConFaltasIds, ...estudiantesConTardanzasIds]),
    ];

    if (todosLosIds.length === 0) {
      return {
        estudiantesConFaltas: [],
        estudiantesConTardanzas: [],
        aulasAfectadas: new Set(),
      };
    }

    console.log(
      `   🔄 Obteniendo datos de ${todosLosIds.length} estudiantes desde Google Drive...`
    );

    // Obtener datos consolidados desde Google Drive
    const { estudiantes: estudiantesMap, aulas: aulasMap } =
      await obtenerDatosEstudiantesYAulasDesdeGoogleDrive(nivel);

    console.log(
      `   ✅ Datos obtenidos: ${estudiantesMap.size} estudiantes, ${aulasMap.size} aulas`
    );

    // Construir objetos de resultado
    const estudiantesConFaltas: EstudianteConProblema[] = [];
    const estudiantesConTardanzas: EstudianteConProblema[] = [];
    const aulasAfectadas = new Set<string>();

    let estudiantesSinDatos = 0;
    let estudiantesSinAula = 0;

    for (const idEstudiante of todosLosIds) {
      // Obtener datos del estudiante desde el mapa
      const estudiante = estudiantesMap.get(idEstudiante);

      if (!estudiante) {
        estudiantesSinDatos++;
        if (estudiantesSinDatos <= 3) {
          console.log(
            `   ⚠️  Estudiante ${idEstudiante} no encontrado en archivos de Google Drive`
          );
        }
        continue;
      }

      // Obtener datos del aula
      const aula = aulasMap.get(estudiante.Id_Aula);

      if (!aula) {
        estudiantesSinAula++;
        if (estudiantesSinAula <= 3) {
          console.log(
            `   ⚠️  Aula ${estudiante.Id_Aula} del estudiante ${idEstudiante} no encontrada`
          );
        }
        continue;
      }

      const baseEstudiante = {
        idEstudiante,
        nombres: estudiante.Nombres,
        apellidos: estudiante.Apellidos,
        grado: aula.Grado,
        seccion: aula.Seccion,
        colorAula: aula.Color,
      };

      aulasAfectadas.add(estudiante.Id_Aula);

      if (estudiantesConFaltasIds.includes(idEstudiante)) {
        estudiantesConFaltas.push({
          ...baseEstudiante,
          diasConsecutivos:
            faltasConsecutivasPorEstudiante.get(idEstudiante) || [],
        });
      }

      if (estudiantesConTardanzasIds.includes(idEstudiante)) {
        estudiantesConTardanzas.push({
          ...baseEstudiante,
          diasConsecutivos:
            tardanzasConsecutivasPorEstudiante.get(idEstudiante) || [],
        });
      }
    }

    if (estudiantesSinDatos > 0) {
      console.log(
        `   ⚠️  Total de estudiantes sin datos: ${estudiantesSinDatos}/${todosLosIds.length}`
      );
    }

    if (estudiantesSinAula > 0) {
      console.log(
        `   ⚠️  Total de estudiantes sin aula: ${estudiantesSinAula}/${todosLosIds.length}`
      );
    }

    console.log(`   📊 Resultados finales construidos:`);
    console.log(
      `      - Estudiantes con faltas agregados: ${estudiantesConFaltas.length}`
    );
    console.log(
      `      - Estudiantes con tardanzas agregados: ${estudiantesConTardanzas.length}`
    );

    // Ordenar por grado, sección y nombre
    const ordenar = (a: EstudianteConProblema, b: EstudianteConProblema) => {
      if (a.grado !== b.grado) return a.grado - b.grado;
      if (a.seccion !== b.seccion) return a.seccion.localeCompare(b.seccion);
      return `${a.apellidos} ${a.nombres}`.localeCompare(
        `${b.apellidos} ${b.nombres}`
      );
    };

    estudiantesConFaltas.sort(ordenar);
    estudiantesConTardanzas.sort(ordenar);

    console.log(
      `   ✅ Análisis completado - ${aulasAfectadas.size} aulas afectadas`
    );

    return {
      estudiantesConFaltas,
      estudiantesConTardanzas,
      aulasAfectadas,
    };
  } catch (error) {
    console.error("❌ Error analizando asistencias consecutivas:", error);
    throw error;
  }
}
