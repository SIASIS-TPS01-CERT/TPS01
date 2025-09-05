import { RegistroAsistenciaExistente } from "../../../../../interfaces/shared/AsistenciasEscolares";
import { ModoRegistro } from "../../../../../interfaces/shared/ModoRegistroPersonal";
import { NivelEducativo } from "../../../../../interfaces/shared/NivelEducativo";
import { MongoOperation } from "../../../../../interfaces/shared/RDP03/MongoOperation";
import { TABLAS_ASISTENCIAS_ESCOLARES } from "../../../../../interfaces/shared/RDP03/TablasDeAsistenciaEscolar";
import { RegistroEstudianteSecundariaRedis } from "../../../../../jobs/asistenciaEscolar/SetAsistenciasYFaltasEstudiantesSecundaria";
import { obtenerFechasActuales } from "../../../../utils/dates/obtenerFechasActuales";
import RDP03_DB_INSTANCES from "../../../connectors/mongodb";

// Interfaz para el resultado del registro
interface ResultadoRegistroSecundaria {
  registrosEntradaGuardados: number;
  registrosSalidaGuardados: number;
  registrosIgnorados: number;
  errores: string[];
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

    // Agrupar registros por tabla, estudiante y modo para optimizar
    const registrosAgrupados = new Map<
      string,
      RegistroEstudianteSecundariaRedis[]
    >();

    for (const registro of registros) {
      const tablaAsistencia =
        TABLAS_ASISTENCIAS_ESCOLARES[NivelEducativo.SECUNDARIA][registro.grado];
      const clave = `${tablaAsistencia}-${registro.idEstudiante}-${registro.modoRegistro}`;

      if (!registrosAgrupados.has(clave)) {
        registrosAgrupados.set(clave, []);
      }
      registrosAgrupados.get(clave)!.push(registro);
    }

    console.log(
      `🔄 Procesando ${registrosAgrupados.size} grupos de registros únicos`
    );

    // Procesar cada grupo
    for (const [clave, grupoRegistros] of registrosAgrupados) {
      // Tomar el registro más reciente del grupo
      const registroMasReciente = grupoRegistros.reduce((max, current) =>
        current.desfaseSegundos < max.desfaseSegundos ? current : max
      );

      const { grado, idEstudiante, modoRegistro, desfaseSegundos } =
        registroMasReciente;

      try {
        // Obtener tabla correspondiente
        const tablaAsistencia =
          TABLAS_ASISTENCIAS_ESCOLARES[NivelEducativo.SECUNDARIA][grado];

        if (!tablaAsistencia) {
          const error = `Tabla no encontrada para grado ${grado}`;
          console.warn(`⚠️ ${error}`);
          errores.push(error);
          registrosIgnorados++;
          continue;
        }

        // Verificar si ya existe un registro para este estudiante y mes
        const operacionBuscar: MongoOperation = {
          operation: "findOne",
          collection: tablaAsistencia,
          filter: {
            Id_Estudiante: idEstudiante,
            Mes: mesActual,
          },
        };

        const registroExistente = (await RDP03_DB_INSTANCES.executeOperation(
          operacionBuscar
        )) as RegistroAsistenciaExistente | null;

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

          // Actualizar registro existente
          const operacionActualizar: MongoOperation = {
            operation: "updateOne",
            collection: tablaAsistencia,
            filter: { _id: registroExistente._id },
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
