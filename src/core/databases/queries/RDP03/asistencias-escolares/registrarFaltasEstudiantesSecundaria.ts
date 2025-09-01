import { CONTROL_ASISTENCIA_DE_SALIDA_SECUNDARIA } from "../../../../../constants/ASISTENCIA_ENTRADA_SALIDA_ESCOLAR";
import { MongoOperation } from "../../../../../interfaces/shared/RDP03/MongoOperation";
import {
  EstudianteActivoSecundaria,
  RegistroEstudianteSecundariaRedis,
} from "../../../../../jobs/asistenciaEscolar/SetAsistenciasYFaltasEstudiantesSecundaria";
import RDP03_DB_INSTANCES from "../../../connectors/mongodb";

// Interfaz para el resultado del registro de faltas
interface ResultadoRegistroFaltas {
  faltasEntradaRegistradas: number;
  faltasSalidaRegistradas: number;
  estudiantesSinEntrada: EstudianteActivoSecundaria[];
  estudiantesSinSalida?: EstudianteActivoSecundaria[];
}

// Interfaz para el registro existente en MongoDB
interface RegistroAsistenciaExistente {
  _id: string;
  Id_Estudiante: string;
  Mes: number;
  Estados: string;
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
      if (registro.modoRegistro === "E") {
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
            "E"
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
            "S"
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
  modoRegistro: "E" | "S"
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

    let estadosActualizados: Record<
      number,
      Record<string, { DesfaseSegundos: number | null }>
    >;

    if (registroExistente) {
      // Ya existe registro para este mes, verificar si ya tiene falta registrada
      try {
        estadosActualizados = JSON.parse(registroExistente.Estados);
      } catch (parseError) {
        console.warn(
          `⚠️ Error parseando estados existentes para estudiante ${estudiante.idEstudiante}, iniciando nuevo registro`
        );
        estadosActualizados = {};
      }

      // Verificar si ya existe registro para este día y modo
      if (
        estadosActualizados[dia] &&
        estadosActualizados[dia][modoRegistro] !== undefined
      ) {
        // Ya existe registro para este día y modo, no sobrescribir
        return false;
      }

      // Agregar falta
      if (!estadosActualizados[dia]) {
        estadosActualizados[dia] = {};
      }
      estadosActualizados[dia][modoRegistro] = {
        DesfaseSegundos: null, // null indica falta
      };

      // Actualizar registro existente
      const operacionActualizar: MongoOperation = {
        operation: "updateOne",
        collection: estudiante.tablaAsistencia,
        filter: { _id: registroExistente._id },
        data: {
          $set: {
            Estados: JSON.stringify(estadosActualizados),
          },
        },
      };

      await RDP03_DB_INSTANCES.executeOperation(operacionActualizar);
    } else {
      // No existe registro para este mes, crear uno nuevo con la falta
      estadosActualizados = {
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
            Estados: JSON.stringify(estadosActualizados),
          },
        },
        options: {
          upsert: true, // Crear si no existe
        },
      };

      await RDP03_DB_INSTANCES.executeOperation(operacionUpsert);
    }

    const tipoRegistro = modoRegistro === "E" ? "entrada" : "salida";
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
