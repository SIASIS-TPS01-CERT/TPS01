import RDP03_DB_INSTANCES from "../../../connectors/mongodb";

import { MongoOperation } from "../../../../../interfaces/shared/RDP03/MongoOperation";
import {
  EstudianteActivoSecundaria,
  RegistroEstudianteSecundariaRedis,
} from "../../../../../jobs/asistenciaEscolar/SetAsistenciasYFaltasEstudiantesSecundaria";
import { CONTROL_ASISTENCIA_DE_SALIDA_SECUNDARIA } from "../../../../../constants/ASISTENCIA_ENTRADA_SALIDA_ESCOLAR";
import { ModoRegistro } from "../../../../../interfaces/shared/ModoRegistroPersonal";
import { RegistroAsistenciaExistente } from "../../../../../interfaces/shared/AsistenciasEscolares";
import { executeMongoDBOperation } from "../../../connectors/mongodb";
import { RolesSistema } from "../../../../../interfaces/shared/RolesSistema";
import { RDP03_Nombres_Tablas } from "../../../../../interfaces/shared/RDP03/RDP03_Tablas";

// Interfaz para el resultado del registro de faltas
interface ResultadoRegistroFaltas {
  faltasEntradaRegistradas: number;
  faltasSalidaRegistradas: number;
  estudiantesSinEntrada: EstudianteActivoSecundaria[];
  estudiantesSinSalida?: EstudianteActivoSecundaria[];
}

/**
 * Obtiene el estado actual de registros de asistencia para múltiples estudiantes
 * OPTIMIZACIÓN: Una sola consulta por tabla en lugar de consultas individuales
 */
async function obtenerEstadoActualEstudiantesParaFaltas(
  estudiantesIds: string[],
  mes: number,
  tablaAsistencia: RDP03_Nombres_Tablas
): Promise<Map<string, RegistroAsistenciaExistente>> {
  try {
    if (estudiantesIds.length === 0) {
      return new Map();
    }

    console.log(
      `🔍 Consultando estado actual de ${estudiantesIds.length} estudiantes en ${tablaAsistencia} para registro de faltas`
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
      `✅ Encontrados ${mapaRegistros.size} registros existentes en ${tablaAsistencia} para análisis de faltas`
    );

    return mapaRegistros;
  } catch (error) {
    console.error(
      `❌ Error obteniendo estado actual de estudiantes para faltas en ${tablaAsistencia}:`,
      error
    );
    return new Map();
  }
}

/**
 * Registra faltas para estudiantes de secundaria que no tuvieron registro ese día
 */
export async function registrarFaltasEstudiantesSecundaria(
  estudiantesActivos: EstudianteActivoSecundaria[],
  registrosProcesados: RegistroEstudianteSecundariaRedis[],
  fechaLocalPeru: Date
): Promise<ResultadoRegistroFaltas> {
  try {
    console.log(
      "📋 Registrando faltas para estudiantes de secundaria sin registro..."
    );

    const mes = fechaLocalPeru.getUTCMonth() + 1;
    const dia = fechaLocalPeru.getUTCDate();

    console.log(`📅 Procesando faltas para mes: ${mes}, día: ${dia}`);

    let faltasEntradaRegistradas = 0;
    let faltasSalidaRegistradas = 0;
    const estudiantesSinEntrada: EstudianteActivoSecundaria[] = [];
    const estudiantesSinSalida: EstudianteActivoSecundaria[] = [];

    // Crear set de estudiantes que tuvieron registro de entrada y salida
    const estudiantesConEntrada = new Set<string>();
    const estudiantesConSalida = new Set<string>();

    for (const registro of registrosProcesados) {
      if (registro.modoRegistro === ModoRegistro.Entrada) {
        estudiantesConEntrada.add(registro.idEstudiante);
      } else if (registro.modoRegistro === "S") {
        estudiantesConSalida.add(registro.idEstudiante);
      }
    }

    console.log(
      `👥 Estudiantes con registro de entrada: ${estudiantesConEntrada.size}`
    );
    if (CONTROL_ASISTENCIA_DE_SALIDA_SECUNDARIA) {
      console.log(
        `👥 Estudiantes con registro de salida: ${estudiantesConSalida.size}`
      );
    }

    // OPTIMIZACIÓN: Agrupar estudiantes por tabla para consultas masivas
    const estudiantesPorTabla = new Map<
      RDP03_Nombres_Tablas,
      EstudianteActivoSecundaria[]
    >();

    for (const estudiante of estudiantesActivos) {
      if (!estudiantesPorTabla.has(estudiante.tablaAsistencia)) {
        estudiantesPorTabla.set(estudiante.tablaAsistencia, []);
      }
      estudiantesPorTabla.get(estudiante.tablaAsistencia)!.push(estudiante);
    }

    console.log(
      `🗂️ Estudiantes agrupados en ${estudiantesPorTabla.size} tablas de asistencia`
    );

    // Procesar cada tabla por separado
    for (const [tablaAsistencia, estudiantes] of estudiantesPorTabla) {
      try {
        console.log(
          `\n📋 Procesando faltas en tabla ${tablaAsistencia} para ${estudiantes.length} estudiantes`
        );

        const idsEstudiantes = estudiantes.map((e) => e.idEstudiante);

        // CLAVE: Una sola consulta masiva por tabla
        const estadoActual = await obtenerEstadoActualEstudiantesParaFaltas(
          idsEstudiantes,
          mes,
          tablaAsistencia
        );

        // Procesar cada estudiante usando el estado consultado masivamente
        for (const estudiante of estudiantes) {
          try {
            // Usar el estado consultado masivamente
            const registroExistente = estadoActual.get(estudiante.idEstudiante);

            // PROCESAR ENTRADA
            if (!estudiantesConEntrada.has(estudiante.idEstudiante)) {
              // Estudiante sin registro de entrada, registrar falta
              const faltaRegistrada = await registrarFaltaIndividual(
                estudiante,
                mes,
                dia,
                ModoRegistro.Entrada,
                registroExistente
              );

              if (faltaRegistrada) {
                faltasEntradaRegistradas++;
                estudiantesSinEntrada.push(estudiante);
              }
            }

            // PROCESAR SALIDA (solo si está habilitado el control)
            if (
              CONTROL_ASISTENCIA_DE_SALIDA_SECUNDARIA &&
              !estudiantesConSalida.has(estudiante.idEstudiante)
            ) {
              // Estudiante sin registro de salida, registrar falta
              const faltaRegistrada = await registrarFaltaIndividual(
                estudiante,
                mes,
                dia,
                ModoRegistro.Salida,
                registroExistente
              );

              if (faltaRegistrada) {
                faltasSalidaRegistradas++;
                estudiantesSinSalida.push(estudiante);
              }
            }
          } catch (error) {
            console.error(
              `❌ Error procesando faltas para estudiante ${estudiante.nombreCompleto} (${estudiante.idEstudiante}):`,
              error
            );
          }
        }
      } catch (error) {
        console.error(
          `❌ Error procesando faltas en tabla ${tablaAsistencia}:`,
          error
        );
      }
    }

    const resultado: ResultadoRegistroFaltas = {
      faltasEntradaRegistradas,
      faltasSalidaRegistradas,
      estudiantesSinEntrada,
    };

    if (CONTROL_ASISTENCIA_DE_SALIDA_SECUNDARIA) {
      resultado.estudiantesSinSalida = estudiantesSinSalida;
    }

    return resultado;
  } catch (error) {
    console.error(
      "❌ Error registrando faltas de estudiantes de secundaria:",
      error
    );
    throw error;
  }
}

/**
 * Registra una falta individual para un estudiante específico
 * OPTIMIZACIÓN: Recibe el registro existente como parámetro (ya consultado masivamente)
 */
async function registrarFaltaIndividual(
  estudiante: EstudianteActivoSecundaria,
  mes: number,
  dia: number,
  modoRegistro: ModoRegistro,
  registroExistente?: RegistroAsistenciaExistente | null
): Promise<boolean> {
  try {
    let asistenciasMensualesActualizadas: Record<
      number,
      Record<string, { DesfaseSegundos: number | null }>
    >;

    if (registroExistente) {
      // Ya existe registro para este mes, verificar si ya tiene falta registrada
      try {
        asistenciasMensualesActualizadas = JSON.parse(
          registroExistente.Asistencias_Mensuales
        );
      } catch (parseError) {
        console.warn(
          `⚠️ Error parseando estados existentes para estudiante ${estudiante.idEstudiante}, iniciando nuevo registro`
        );
        asistenciasMensualesActualizadas = {};
      }

      // Verificar si ya existe registro para este día y modo
      if (
        asistenciasMensualesActualizadas[dia] &&
        asistenciasMensualesActualizadas[dia][modoRegistro] !== undefined
      ) {
        // Ya existe registro para este día y modo, no sobrescribir
        return false;
      }

      // Agregar falta
      if (!asistenciasMensualesActualizadas[dia]) {
        asistenciasMensualesActualizadas[dia] = {};
      }
      asistenciasMensualesActualizadas[dia][modoRegistro] = {
        DesfaseSegundos: null, // null indica falta
      };

      // Actualizar registro existente en TODAS las instancias
      const operacionActualizar: MongoOperation = {
        operation: "updateOne",
        collection: estudiante.tablaAsistencia,
        filter: {
          Id_Estudiante: estudiante.idEstudiante,
          Mes: mes,
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
    } else {
      // No existe registro para este mes, crear uno nuevo con la falta
      asistenciasMensualesActualizadas = {
        [dia]: {
          [modoRegistro]: {
            DesfaseSegundos: null, // null indica falta
          },
        },
      };

      const operacionUpsert: MongoOperation = {
        operation: "updateOne",
        collection: estudiante.tablaAsistencia,
        filter: {
          Id_Estudiante: estudiante.idEstudiante,
          Mes: mes,
        },
        data: {
          $set: {
            Id_Estudiante: estudiante.idEstudiante,
            Mes: mes,
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
    }

    const tipoRegistro =
      modoRegistro === ModoRegistro.Entrada ? "entrada" : "salida";
    console.log(
      `❌ Falta de ${tipoRegistro} registrada para ${estudiante.nombreCompleto} (${estudiante.idEstudiante}) en día ${dia}`
    );

    return true;
  } catch (error) {
    console.error(
      `❌ Error registrando falta individual para estudiante ${estudiante.idEstudiante}:`,
      error
    );
    return false;
  }
}
