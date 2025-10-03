import { RegistroAsistenciaExistente } from "../../../../../interfaces/shared/AsistenciasEscolares";
import { ModoRegistro } from "../../../../../interfaces/shared/ModoRegistroPersonal";
import { NivelEducativo } from "../../../../../interfaces/shared/NivelEducativo";
import { MongoOperation } from "../../../../../interfaces/shared/RDP03/MongoOperation";
import { TABLAS_ASISTENCIAS_ESCOLARES } from "../../../../../interfaces/shared/RDP03/TablasDeAsistenciaEscolar";
import { RegistroEstudianteSecundariaRedis } from "../../../../../jobs/asistenciaEscolar/SetAsistenciasYFaltasEstudiantesSecundaria";
import { obtenerFechasActuales } from "../../../../utils/dates/obtenerFechasActuales";
import RDP03_DB_INSTANCES from "../../../connectors/mongodb";
import { executeMongoDBOperation } from "../../../connectors/mongodb";
import { RolesSistema } from "../../../../../interfaces/shared/RolesSistema";
import { RDP03_Nombres_Tablas } from "../../../../../interfaces/shared/RDP03/RDP03_Tablas";

// Interfaz para el resultado del registro
interface ResultadoRegistroSecundaria {
  registrosEntradaGuardados: number;
  registrosSalidaGuardados: number;
  registrosIgnorados: number;
  errores: string[];
}

/**
 * Obtiene el estado actual de registros de asistencia para múltiples estudiantes
 * OPTIMIZACIÓN: Una sola consulta por tabla en lugar de consultas individuales
 */
async function obtenerEstadoActualEstudiantes(
  estudiantesIds: string[],
  mes: number,
  tablaAsistencia: RDP03_Nombres_Tablas
): Promise<Map<string, RegistroAsistenciaExistente>> {
  try {
    if (estudiantesIds.length === 0) {
      return new Map();
    }

    console.log(
      `🔍 Consultando estado actual de ${estudiantesIds.length} estudiantes en ${tablaAsistencia}`
    );

    const operacionBuscarTodos: MongoOperation = {
      operation: "find",
      collection: tablaAsistencia,
      filter: {
        Id_Estudiante: { $in: estudiantesIds },
        Mes: mes,
      },
    };

    // CLAVE: Una sola consulta a una instancia específica (no aleatoria)
    const registrosExistentes = (await executeMongoDBOperation(
      operacionBuscarTodos,
      { role: RolesSistema.Directivo } // Usar grupo específico para consistencia
    )) as RegistroAsistenciaExistente[];

    // Crear mapa para acceso rápido por ID de estudiante
    const mapaRegistros = new Map<string, RegistroAsistenciaExistente>();
    registrosExistentes.forEach((registro: RegistroAsistenciaExistente) => {
      mapaRegistros.set(registro.Id_Estudiante, registro);
    });

    console.log(
      `✅ Encontrados ${mapaRegistros.size} registros existentes en ${tablaAsistencia}`
    );

    return mapaRegistros;
  } catch (error) {
    console.error(
      `❌ Error obteniendo estado actual de estudiantes en ${tablaAsistencia}:`,
      error
    );
    return new Map();
  }
}

/**
 * Registra asistencias de estudiantes de secundaria desde Redis en MongoDB
 */
export async function registrarAsistenciasEstudiantesSecundariaDesdeRedis(
  registros: RegistroEstudianteSecundariaRedis[]
): Promise<ResultadoRegistroSecundaria> {
  try {
    console.log(
      "💾 Persistiendo asistencias de estudiantes de secundaria en MongoDB..."
    );

    if (!Array.isArray(registros) || registros.length === 0) {
      console.log("ℹ️ No hay registros de secundaria para procesar");
      return {
        registrosEntradaGuardados: 0,
        registrosSalidaGuardados: 0,
        registrosIgnorados: 0,
        errores: [],
      };
    }

    const { fechaLocalPeru } = obtenerFechasActuales();
    const mesActual = fechaLocalPeru.getUTCMonth() + 1;
    const diaActual = fechaLocalPeru.getUTCDate();

    console.log(
      `📅 Procesando registros para mes ${mesActual}, día ${diaActual}`
    );
    console.log(`📊 Total registros a procesar: ${registros.length}`);

    let registrosEntradaGuardados = 0;
    let registrosSalidaGuardados = 0;
    let registrosIgnorados = 0;
    const errores: string[] = [];

    // OPTIMIZACIÓN: Agrupar registros por tabla para consultas masivas
    const registrosPorTabla = new Map<
      RDP03_Nombres_Tablas,
      {
        estudiantes: Set<string>;
        registros: RegistroEstudianteSecundariaRedis[];
      }
    >();

    // Agrupar y deduplicar registros
    for (const registro of registros) {
      const tablaAsistencia =
        TABLAS_ASISTENCIAS_ESCOLARES[NivelEducativo.SECUNDARIA][registro.grado];

      if (!tablaAsistencia) {
        const error = `Tabla no encontrada para grado ${registro.grado}`;
        console.warn(`⚠️ ${error}`);
        errores.push(error);
        registrosIgnorados++;
        continue;
      }

      if (!registrosPorTabla.has(tablaAsistencia)) {
        registrosPorTabla.set(tablaAsistencia, {
          estudiantes: new Set(),
          registros: [],
        });
      }

      const datosTabla = registrosPorTabla.get(tablaAsistencia)!;
      datosTabla.estudiantes.add(registro.idEstudiante);
      datosTabla.registros.push(registro);
    }

    console.log(
      `🗂️ Registros agrupados en ${registrosPorTabla.size} tablas de asistencia`
    );

    // Procesar cada tabla por separado
    for (const [tablaAsistencia, datosTabla] of registrosPorTabla) {
      try {
        console.log(
          `\n📋 Procesando tabla ${tablaAsistencia} con ${datosTabla.registros.length} registros`
        );

        // CLAVE: Una sola consulta masiva por tabla
        const estadoActual = await obtenerEstadoActualEstudiantes(
          Array.from(datosTabla.estudiantes),
          mesActual,
          tablaAsistencia
        );

        // Agrupar registros por estudiante y modo para deduplicar
        const registrosAgrupados = new Map<
          string,
          RegistroEstudianteSecundariaRedis[]
        >();

        for (const registro of datosTabla.registros) {
          const clave = `${registro.idEstudiante}-${registro.modoRegistro}`;

          if (!registrosAgrupados.has(clave)) {
            registrosAgrupados.set(clave, []);
          }
          registrosAgrupados.get(clave)!.push(registro);
        }

        console.log(
          `🔄 Procesando ${registrosAgrupados.size} grupos de registros únicos en ${tablaAsistencia}`
        );

        // Procesar cada grupo de registros
        for (const [clave, grupoRegistros] of registrosAgrupados) {
          // Tomar el registro más reciente del grupo
          const registroMasReciente = grupoRegistros.reduce((max, current) =>
            current.desfaseSegundos < max.desfaseSegundos ? current : max
          );

          const { idEstudiante, modoRegistro, desfaseSegundos } =
            registroMasReciente;

          try {
            // Usar el estado consultado masivamente
            const registroExistente = estadoActual.get(idEstudiante);

            let asistenciasMensualesActualizadas: Record<
              number,
              Record<string, { DesfaseSegundos: number }>
            >;

            if (registroExistente) {
              // Ya existe registro para este mes, actualizarlo
              try {
                asistenciasMensualesActualizadas = JSON.parse(
                  registroExistente.Asistencias_Mensuales
                );
              } catch (parseError) {
                console.warn(
                  `⚠️ Error parseando estados existentes para estudiante ${idEstudiante}, iniciando nuevo registro`
                );
                asistenciasMensualesActualizadas = {};
              }

              // Verificar si ya existe registro para este día y modo
              if (
                asistenciasMensualesActualizadas[diaActual] &&
                asistenciasMensualesActualizadas[diaActual][modoRegistro]
              ) {
                console.log(
                  `ℹ️ Ya existe registro para estudiante ${idEstudiante} en día ${diaActual}, modo ${modoRegistro}, manteniendo existente`
                );
                registrosIgnorados++;
                continue;
              }

              // Agregar nuevo registro
              if (!asistenciasMensualesActualizadas[diaActual]) {
                asistenciasMensualesActualizadas[diaActual] = {};
              }
              asistenciasMensualesActualizadas[diaActual][modoRegistro] = {
                DesfaseSegundos: desfaseSegundos,
              };

              // Actualizar registro existente en TODAS las instancias
              const operacionActualizar: MongoOperation = {
                operation: "updateOne",
                collection: tablaAsistencia,
                filter: {
                  Id_Estudiante: idEstudiante,
                  Mes: mesActual,
                },
                data: {
                  $set: {
                    Asistencias_Mensuales: JSON.stringify(
                      asistenciasMensualesActualizadas
                    ),
                  },
                },
              };

              await RDP03_DB_INSTANCES.executeOperation(operacionActualizar);

              console.log(
                `✅ Registro actualizado para estudiante ${idEstudiante} en tabla ${tablaAsistencia}`
              );
            } else {
              // No existe registro para este mes, crear uno nuevo
              asistenciasMensualesActualizadas = {
                [diaActual]: {
                  [modoRegistro]: {
                    DesfaseSegundos: desfaseSegundos,
                  },
                },
              };

              const operacionUpsert: MongoOperation = {
                operation: "updateOne",
                collection: tablaAsistencia,
                filter: {
                  Id_Estudiante: idEstudiante,
                  Mes: mesActual,
                },
                data: {
                  $set: {
                    Id_Estudiante: idEstudiante,
                    Mes: mesActual,
                    Asistencias_Mensuales: JSON.stringify(
                      asistenciasMensualesActualizadas
                    ),
                  },
                },
                options: {
                  upsert: true, // Crear si no existe
                },
              };

              await RDP03_DB_INSTANCES.executeOperation(operacionUpsert);

              console.log(
                `✅ Nuevo registro creado para estudiante ${idEstudiante} en tabla ${tablaAsistencia}`
              );
            }

            // Contar estadísticas
            if (modoRegistro === ModoRegistro.Entrada) {
              registrosEntradaGuardados++;
            } else {
              registrosSalidaGuardados++;
            }

            // Log para registros duplicados
            if (grupoRegistros.length > 1) {
              console.log(
                `🔄 Se procesaron ${grupoRegistros.length} registros duplicados para estudiante ${idEstudiante}-${modoRegistro}, se tomó el más reciente`
              );
            }
          } catch (error) {
            const mensajeError = `Error procesando registro para estudiante ${idEstudiante}: ${
              error instanceof Error ? error.message : String(error)
            }`;
            console.error(`❌ ${mensajeError}`);
            errores.push(mensajeError);
            registrosIgnorados++;
          }
        }
      } catch (error) {
        const mensajeError = `Error procesando tabla ${tablaAsistencia}: ${
          error instanceof Error ? error.message : String(error)
        }`;
        console.error(`❌ ${mensajeError}`);
        errores.push(mensajeError);
      }
    }

    console.log(
      "\n=== 📊 Resumen de persistencia de asistencias de estudiantes de secundaria ==="
    );
    console.log(
      `📥 Registros de entrada guardados: ${registrosEntradaGuardados}`
    );
    console.log(
      `📤 Registros de salida guardados: ${registrosSalidaGuardados}`
    );
    console.log(`⏭️ Registros ignorados: ${registrosIgnorados}`);

    if (errores.length > 0) {
      console.log(`❌ Errores encontrados: ${errores.length}`);
      errores.slice(0, 3).forEach((error) => console.log(`   - ${error}`));
      if (errores.length > 3) {
        console.log(`   ... y ${errores.length - 3} errores más`);
      }
    }

    return {
      registrosEntradaGuardados,
      registrosSalidaGuardados,
      registrosIgnorados,
      errores,
    };
  } catch (error) {
    const mensajeError = `Error general al persistir asistencias de estudiantes de secundaria: ${
      error instanceof Error ? error.message : String(error)
    }`;
    console.error(`❌ ${mensajeError}`);
    throw new Error(mensajeError);
  }
}
