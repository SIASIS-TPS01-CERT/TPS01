import RDP03_DB_INSTANCES from "../../../connectors/mongodb";

import { MongoOperation } from "../../../../../interfaces/shared/RDP03/MongoOperation";
import {
  EstudianteActivoSecundaria,
  RegistroEstudianteSecundariaRedis,
} from "../../../../../jobs/asistenciaEscolar/SetAsistenciasYFaltasEstudiantesSecundaria";
import { CONTROL_ASISTENCIA_DE_SALIDA_SECUNDARIA } from "../../../../../constants/ASISTENCIA_ENTRADA_SALIDA_ESCOLAR";
import { ModoRegistro } from "../../../../../interfaces/shared/ModoRegistroPersonal";
import { RegistroAsistenciaExistente } from "../../../../../interfaces/shared/AsistenciasEscolares";

// Interfaz para el resultado del registro de faltas
interface ResultadoRegistroFaltas {
  faltasEntradaRegistradas: number;
  faltasSalidaRegistradas: number;
  estudiantesSinEntrada: EstudianteActivoSecundaria[];
  estudiantesSinSalida?: EstudianteActivoSecundaria[];
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

    // Procesar cada estudiante activo
    for (const estudiante of estudiantesActivos) {
      try {
        // PROCESAR ENTRADA
        if (!estudiantesConEntrada.has(estudiante.idEstudiante)) {
          // Estudiante sin registro de entrada, registrar falta
          const faltaRegistrada = await registrarFaltaIndividual(
            estudiante,
            mes,
            dia,
            ModoRegistro.Entrada
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
            ModoRegistro.Salida
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
 */
async function registrarFaltaIndividual(
  estudiante: EstudianteActivoSecundaria,
  mes: number,
  dia: number,
  modoRegistro: ModoRegistro
): Promise<boolean> {
  try {
    // Verificar si ya existe un registro para este estudiante y mes
    const operacionBuscar: MongoOperation = {
      operation: "findOne",
      collection: estudiante.tablaAsistencia,
      filter: {
        Id_Estudiante: estudiante.idEstudiante,
        Mes: mes,
      },
    };

    const registroExistente = (await RDP03_DB_INSTANCES.executeOperation(
      operacionBuscar
    )) as RegistroAsistenciaExistente | null;

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

      // Actualizar registro existente usando Id_Estudiante + Mes (no _id)
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
