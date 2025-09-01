import { NivelEducativo } from "../../../../../interfaces/shared/NivelEducativo";
import { MongoOperation } from "../../../../../interfaces/shared/RDP03/MongoOperation";
import { RDP03_Nombres_Tablas } from "../../../../../interfaces/shared/RDP03/RDP03_Tablas";
import { TABLAS_ASISTENCIAS_ESCOLARES } from "../../../../../interfaces/shared/RDP03/TablasDeAsistenciaEscolar";
import { obtenerFechasActuales } from "../../../../utils/dates/obtenerFechasActuales";
import RDP03_DB_INSTANCES from "../../../connectors/mongodb";

// Interfaz para estudiante inactivo
interface EstudianteInactivo {
  Id_Estudiante: string;
  Nombres: string;
  Apellidos: string;
  grado: number;
  nivel: NivelEducativo;
  tablaAsistencia: string;
  nombreCompleto: string;
}

// Interfaz para el resultado del registro
interface ResultadoRegistroFaltasEstudiantesInactivos {
  totalEstudiantesInactivos: number;
  faltasCompletasRegistradas: number;
  errores: number;
}

/**
 * Obtiene todos los estudiantes inactivos (Estado = false) de todas las aulas
 */
async function obtenerEstudiantesInactivos(): Promise<EstudianteInactivo[]> {
  try {
    console.log("📋 Obteniendo estudiantes inactivos desde MongoDB...");

    // Obtener todos los estudiantes inactivos
    const operacionBuscar: MongoOperation = {
      operation: "find",
      collection: "T_Estudiantes",
      filter: {
        Estado: false, // Solo estudiantes inactivos
      },
      options: {
        projection: {
          Id_Estudiante: 1,
          Nombres: 1,
          Apellidos: 1,
          Id_Aula: 1,
          _id: 0,
        },
      },
    };

    const estudiantesInactivos = await RDP03_DB_INSTANCES.executeOperation(
      operacionBuscar
    );

    if (!estudiantesInactivos || estudiantesInactivos.length === 0) {
      console.log("ℹ️ No se encontraron estudiantes inactivos");
      return [];
    }

    console.log(
      `👥 Encontrados ${estudiantesInactivos.length} estudiantes inactivos`
    );

    // Obtener información de las aulas para determinar grado y nivel
    const idsAulas = [
      ...new Set(
        estudiantesInactivos.map((e: any) => e.Id_Aula).filter(Boolean)
      ),
    ];

    const operacionAulas: MongoOperation = {
      operation: "find",
      collection: "T_Aulas",
      filter: {
        Id_Aula: { $in: idsAulas },
      },
      options: {
        projection: {
          Id_Aula: 1,
          Nivel: 1,
          Grado: 1,
          _id: 0,
        },
      },
    };

    const aulas = await RDP03_DB_INSTANCES.executeOperation(operacionAulas);
    const aulaMap = new Map();

    if (aulas) {
      aulas.forEach((aula: any) => {
        aulaMap.set(aula.Id_Aula, {
          nivel: aula.Nivel as NivelEducativo,
          grado: aula.Grado,
        });
      });
    }

    // Procesar estudiantes inactivos
    const estudiantesInactivosProcesados: EstudianteInactivo[] = [];

    for (const estudiante of estudiantesInactivos) {
      const infoAula = aulaMap.get(estudiante.Id_Aula);

      if (!infoAula) {
        console.warn(
          `⚠️ No se encontró información de aula para estudiante ${estudiante.Id_Estudiante}`
        );
        continue;
      }

      // Obtener tabla de asistencia correspondiente
      const tablaAsistencia =
        TABLAS_ASISTENCIAS_ESCOLARES[infoAula.nivel as NivelEducativo][
          infoAula.grado
        ];

      if (!tablaAsistencia) {
        console.warn(
          `⚠️ No se encontró tabla de asistencia para ${infoAula.nivel} grado ${infoAula.grado}`
        );
        continue;
      }

      estudiantesInactivosProcesados.push({
        Id_Estudiante: estudiante.Id_Estudiante,
        Nombres: estudiante.Nombres,
        Apellidos: estudiante.Apellidos,
        grado: infoAula.grado,
        nivel: infoAula.nivel,
        tablaAsistencia: tablaAsistencia,
        nombreCompleto: `${estudiante.Nombres} ${estudiante.Apellidos}`,
      });
    }

    console.log(
      `✅ Procesados ${estudiantesInactivosProcesados.length} estudiantes inactivos con información completa`
    );

    // Mostrar resumen por nivel y grado
    const resumenPorNivelGrado = estudiantesInactivosProcesados.reduce(
      (acc, estudiante) => {
        const clave = `${estudiante.nivel} - Grado ${estudiante.grado}`;
        acc[clave] = (acc[clave] || 0) + 1;
        return acc;
      },
      {} as Record<string, number>
    );

    console.log("📊 Resumen por nivel y grado:");
    Object.entries(resumenPorNivelGrado).forEach(([nivelGrado, cantidad]) => {
      console.log(`   ${nivelGrado}: ${cantidad} estudiantes inactivos`);
    });

    return estudiantesInactivosProcesados;
  } catch (error) {
    console.error("❌ Error obteniendo estudiantes inactivos:", error);
    return [];
  }
}

/**
 * Registra falta completa (todo el día) para un estudiante inactivo específico
 */
async function registrarFaltaCompletaEstudianteInactivo(
  estudiante: EstudianteInactivo,
  mes: number,
  dia: number
): Promise<boolean> {
  try {
    // Verificar si ya existe un registro para este estudiante y mes
    const operacionBuscar: MongoOperation = {
      operation: "findOne",
      collection: estudiante.tablaAsistencia as RDP03_Nombres_Tablas,
      filter: {
        Id_Estudiante: estudiante.Id_Estudiante,
        Mes: mes,
      },
    };

    const registroExistente = await RDP03_DB_INSTANCES.executeOperation(
      operacionBuscar
    );

    let estadosActualizados: Record<number, null>;

    if (registroExistente) {
      // Ya existe registro para este mes, verificar si ya tiene falta registrada
      try {
        estadosActualizados = JSON.parse(registroExistente.Estados);
      } catch (parseError) {
        console.warn(
          `⚠️ Error parseando estados existentes para estudiante ${estudiante.Id_Estudiante}, iniciando nuevo registro`
        );
        estadosActualizados = {};
      }

      // Verificar si ya existe registro para este día
      if (estadosActualizados[dia] !== undefined) {
        // Ya existe registro para este día, no sobrescribir
        return false;
      }

      // Agregar falta completa para el día
      estadosActualizados[dia] = null; // null indica falta completa del día

      // Actualizar registro existente
      const operacionActualizar: MongoOperation = {
        operation: "updateOne",
        collection: estudiante.tablaAsistencia as RDP03_Nombres_Tablas,
        filter: { _id: registroExistente._id },
        data: {
          $set: {
            Estados: JSON.stringify(estadosActualizados),
          },
        },
      };

      await RDP03_DB_INSTANCES.executeOperation(operacionActualizar);
    } else {
      // No existe registro para este mes, crear uno nuevo con la falta completa
      estadosActualizados = {
        [dia]: null, // null indica falta completa del día
      };

      const operacionUpsert: MongoOperation = {
        operation: "updateOne",
        collection: estudiante.tablaAsistencia as RDP03_Nombres_Tablas,
        filter: {
          Id_Estudiante: estudiante.Id_Estudiante,
          Mes: mes,
        },
        data: {
          $set: {
            Id_Estudiante: estudiante.Id_Estudiante,
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

    console.log(
      `❌ Falta completa registrada para estudiante inactivo ${estudiante.nombreCompleto} (${estudiante.Id_Estudiante}) en día ${dia}`
    );

    return true;
  } catch (error) {
    console.error(
      `❌ Error registrando falta completa para estudiante inactivo ${estudiante.Id_Estudiante}:`,
      error
    );
    return false;
  }
}

/**
 * Registra faltas automáticas para todos los estudiantes inactivos
 */
export async function registrarFaltasAutomaticasEstudiantesInactivos(): Promise<ResultadoRegistroFaltasEstudiantesInactivos> {
  try {
    console.log(
      "🔄 Iniciando registro de faltas automáticas para estudiantes inactivos..."
    );

    const { fechaLocalPeru } = obtenerFechasActuales();
    const mes = fechaLocalPeru.getUTCMonth() + 1;
    const dia = fechaLocalPeru.getUTCDate();

    console.log(`📅 Procesando faltas completas para mes: ${mes}, día: ${dia}`);

    // Obtener estudiantes inactivos
    const estudiantesInactivos = await obtenerEstudiantesInactivos();

    if (estudiantesInactivos.length === 0) {
      return {
        totalEstudiantesInactivos: 0,
        faltasCompletasRegistradas: 0,
        errores: 0,
      };
    }

    let faltasCompletasRegistradas = 0;
    let errores = 0;

    // Procesar cada estudiante inactivo
    for (const estudiante of estudiantesInactivos) {
      try {
        // REGISTRAR FALTA COMPLETA DEL DÍA (formato: {28: null})
        const faltaRegistrada = await registrarFaltaCompletaEstudianteInactivo(
          estudiante,
          mes,
          dia
        );

        if (faltaRegistrada) {
          faltasCompletasRegistradas++;
        }
      } catch (error) {
        console.error(
          `❌ Error procesando estudiante inactivo ${estudiante.nombreCompleto}:`,
          error
        );
        errores++;
      }
    }

    console.log(
      "\n📊 === Resumen de registro de faltas para estudiantes inactivos ==="
    );
    console.log(
      `👥 Total estudiantes inactivos procesados: ${estudiantesInactivos.length}`
    );
    console.log(
      `❌ Faltas completas registradas: ${faltasCompletasRegistradas}`
    );
    console.log(`❌ Errores: ${errores}`);

    return {
      totalEstudiantesInactivos: estudiantesInactivos.length,
      faltasCompletasRegistradas,
      errores,
    };
  } catch (error) {
    console.error(
      "❌ Error en registro de faltas automáticas para estudiantes inactivos:",
      error
    );
    throw error;
  }
}
