import { ModoRegistro } from "../../../../../interfaces/shared/ModoRegistroPersonal";
import { RolesSistema } from "../../../../../interfaces/shared/RolesSistema";
import { obtenerFechasActuales } from "../../../../utils/dates/obtenerFechasActuales";
import RDP02_DB_INSTANCES from "../../../connectors/postgres";
import { RegistroPersonalRedis } from "../../RDP05/obtenerRegistrosAsistenciaPersonalRedis";

// Interfaz para la configuración de tabla
interface ConfiguracionTabla {
  tabla: string;
  campoID: string;
  campoJSON: "Entradas" | "Salidas";
  esDirectivo?: boolean; // 🆕 NUEVO CAMPO para identificar directivos
}

// Interfaz para el resultado del registro
interface ResultadoRegistro {
  registrosEntradaGuardados: number;
  registrosSalidaGuardados: number;
  registrosIgnorados: number;
  errores: string[];
}

/**
 * Obtiene la configuración de tabla y campos según el rol y modo de registro
 */
function obtenerConfiguracionTabla(
  rol: string,
  modoRegistro: string
): ConfiguracionTabla | null {
  const esEntrada = modoRegistro === ModoRegistro.Entrada;

  // Validar que el modo de registro sea válido
  if (!Object.values(ModoRegistro).includes(modoRegistro as ModoRegistro)) {
    console.warn(`⚠️  Modo de registro inválido: ${modoRegistro}`);
    return null;
  }

  switch (rol) {
    // 🆕 NUEVO CASO PARA DIRECTIVOS
    case RolesSistema.Directivo:
      return {
        tabla: esEntrada
          ? "T_Control_Entrada_Mensual_Directivos"
          : "T_Control_Salida_Mensual_Directivos",
        campoID: "Id_Directivo", // Directivos usan Id_Directivo
        campoJSON: esEntrada ? "Entradas" : "Salidas",
        esDirectivo: true, // 🆕 Marcador especial
      };

    case RolesSistema.ProfesorPrimaria:
      return {
        tabla: esEntrada
          ? "T_Control_Entrada_Mensual_Profesores_Primaria"
          : "T_Control_Salida_Mensual_Profesores_Primaria",
        campoID: "Id_Profesor_Primaria",
        campoJSON: esEntrada ? "Entradas" : "Salidas",
      };

    case RolesSistema.ProfesorSecundaria:
    case RolesSistema.Tutor:
      return {
        tabla: esEntrada
          ? "T_Control_Entrada_Mensual_Profesores_Secundaria"
          : "T_Control_Salida_Mensual_Profesores_Secundaria",
        campoID: "Id_Profesor_Secundaria",
        campoJSON: esEntrada ? "Entradas" : "Salidas",
      };

    case RolesSistema.Auxiliar:
      return {
        tabla: esEntrada
          ? "T_Control_Entrada_Mensual_Auxiliar"
          : "T_Control_Salida_Mensual_Auxiliar",
        campoID: "Id_Auxiliar",
        campoJSON: esEntrada ? "Entradas" : "Salidas",
      };

    case RolesSistema.PersonalAdministrativo:
      return {
        tabla: esEntrada
          ? "T_Control_Entrada_Mensual_Personal_Administrativo"
          : "T_Control_Salida_Mensual_Personal_Administrativo",
        campoID: "Id_Personal_Administrativo",
        campoJSON: esEntrada ? "Entradas" : "Salidas",
      };

    default:
      console.warn(`⚠️  Rol no reconocido: ${rol}`);
      return null;
  }
}

/**
 * Función para registrar asistencia con valores personalizados (timestamp y desfase)
 * 🆕 ACTUALIZADA para manejar directivos con Id_Directivo
 */
async function registrarAsistenciaConValoresPersonalizados(
  tabla: string,
  campoID: string,
  identificador: string | number, // 🆕 Puede ser ID (string) o Id_Directivo (number)
  mes: number,
  dia: number,
  campoJson: "Entradas" | "Salidas",
  valorRegistro: { Timestamp: number; DesfaseSegundos: number },
  rol: string,
  esDirectivo: boolean = false // 🆕 NUEVO PARÁMETRO
): Promise<{ exito: boolean; mensaje: string }> {
  try {
    // Verificar si ya existe un registro para este mes
    const sqlVerificar = `
      SELECT *
      FROM "${tabla}"
      WHERE "${campoID}" = $1 AND "Mes" = $2
    `;
    const resultVerificar = await RDP02_DB_INSTANCES.query(
      sqlVerificar,
      [identificador, mes],
      false
    );

    if (resultVerificar.rows.length > 0) {
      // Ya existe un registro para este mes, actualizarlo
      const registro = resultVerificar.rows[0];

      // Encontrar la clave correcta para el JSON
      const jsonKey = Object.keys(registro).find(
        (key) => key.toLowerCase() === campoJson.toLowerCase()
      );

      if (!jsonKey) {
        return {
          exito: false,
          mensaje: `Campo ${campoJson} no encontrado en el registro`,
        };
      }

      // Obtener el JSON actual o inicializar uno nuevo
      const jsonActual = registro[jsonKey] || {};

      // Solo actualizar si no existe un registro para este día
      if (jsonActual[dia.toString()] === undefined) {
        // Asignar el valor personalizado para este día
        jsonActual[dia.toString()] = valorRegistro;

        // Obtener el ID para actualización
        const idKey = Object.keys(registro).find((key) =>
          key.toLowerCase().includes("id")
        );

        if (!idKey) {
          return {
            exito: false,
            mensaje:
              "No se pudo determinar la columna ID para la actualización",
          };
        }

        // Actualizar el registro
        const sqlActualizar = `
          UPDATE "${tabla}"
          SET "${campoJson}" = $1
          WHERE "${idKey}" = $2
        `;
        await RDP02_DB_INSTANCES.query(sqlActualizar, [
          JSON.stringify(jsonActual),
          registro[idKey],
        ]);

        const tipoIdentificador = esDirectivo ? "Id_Directivo" : "Identificador_Nacional";
        return {
          exito: true,
          mensaje: `Registro actualizado para ${tipoIdentificador} ${identificador} en día ${dia}`,
        };
      } else {
        const tipoIdentificador = esDirectivo ? "Id_Directivo" : "Identificador_Nacional";
        return {
          exito: false,
          mensaje: `Registro ya existe para ${tipoIdentificador} ${identificador} en día ${dia}, manteniendo el existente`,
        };
      }
    } else {
      // No existe registro para este mes, crearlo
      const nuevoJson: any = {};
      nuevoJson[dia.toString()] = valorRegistro;

      // Insertar nuevo registro
      const sqlInsertar = `
        INSERT INTO "${tabla}" ("${campoID}", "Mes", "${campoJson}")
        VALUES ($1, $2, $3)
      `;
      await RDP02_DB_INSTANCES.query(sqlInsertar, [
        identificador,
        mes,
        JSON.stringify(nuevoJson),
      ]);

      const tipoIdentificador = esDirectivo ? "Id_Directivo" : "Identificador_Nacional";
      return {
        exito: true,
        mensaje: `Nuevo registro creado para ${tipoIdentificador} ${identificador} en mes ${mes}`,
      };
    }
  } catch (error) {
    console.error(`❌ Error al registrar asistencia en tabla ${tabla}:`, error);
    return {
      exito: false,
      mensaje: `Error en base de datos: ${
        error instanceof Error ? error.message : String(error)
      }`,
    };
  }
}

/**
 * Persiste los registros de asistencia de personal de Redis en las tablas JSON mensuales correspondientes
 * 🆕 ACTUALIZADA con soporte completo para directivos
 */
export async function registrarAsistenciasUnitariasDePersonalDesdeRedis(
  registros: RegistroPersonalRedis[]
): Promise<ResultadoRegistro> {
  try {
    console.log(
      "💾 Persistiendo registros de asistencia de personal en las tablas JSON mensuales..."
    );

    // Validación de entrada
    if (!Array.isArray(registros)) {
      throw new Error("El parámetro registros debe ser un array");
    }

    if (registros.length === 0) {
      console.log("ℹ️  No hay registros para procesar");
      return {
        registrosEntradaGuardados: 0,
        registrosSalidaGuardados: 0,
        registrosIgnorados: 0,
        errores: [],
      };
    }

    let registrosEntradaGuardados = 0;
    let registrosSalidaGuardados = 0;
    let registrosIgnorados = 0;
    const errores: string[] = [];

    // Obtener el mes y día usando tu función de fechas actuales
    const { fechaLocalPeru } = obtenerFechasActuales();
    const mesActual = fechaLocalPeru.getUTCMonth() + 1;
    const diaActual = fechaLocalPeru.getUTCDate();

    console.log(
      `📅 Procesando registros para el mes ${mesActual}, día ${diaActual}`
    );
    console.log(`📊 Total de registros a procesar: ${registros.length}`);

    // Separar registros de directivos de otros roles
    const registrosDirectivos = registros.filter(
      (r) => r.rol === RolesSistema.Directivo
    );
    const registrosOtrosRoles = registros.filter(
      (r) => r.rol !== RolesSistema.Directivo
    );

    console.log(`🏢 Registros de directivos: ${registrosDirectivos.length}`);
    console.log(`👥 Registros de otros roles: ${registrosOtrosRoles.length}`);

    // Agrupar registros no-directivos por ID y modo para optimizar
    const registrosAgrupadosNormales = new Map<
      string,
      RegistroPersonalRedis[]
    >();
    for (const registro of registrosOtrosRoles) {
      const clave = `${registro.id}-${registro.modoRegistro}`;
      if (!registrosAgrupadosNormales.has(clave)) {
        registrosAgrupadosNormales.set(clave, []);
      }
      registrosAgrupadosNormales.get(clave)!.push(registro);
    }

    // 🆕 PROCESAR DIRECTIVOS (ya vienen con Id_Directivo desde Redis)
    const registrosAgrupadosDirectivos = new Map<
      string,
      RegistroPersonalRedis[]
    >();
    for (const registro of registrosDirectivos) {
      const clave = `${registro.id}-${registro.modoRegistro}`; // ID contiene Id_Directivo como string
      if (!registrosAgrupadosDirectivos.has(clave)) {
        registrosAgrupadosDirectivos.set(clave, []);
      }
      registrosAgrupadosDirectivos.get(clave)!.push(registro);
    }

    console.log(
      `🔄 Procesando ${registrosAgrupadosNormales.size} grupos normales + ${registrosAgrupadosDirectivos.size} grupos de directivos`
    );

    // PROCESAR REGISTROS NORMALES (sin conversión)
    for (const [clave, grupoRegistros] of registrosAgrupadosNormales) {
      const registroMasReciente = grupoRegistros.reduce((max, current) =>
        current.timestamp > max.timestamp ? current : max
      );

      const { modoRegistro, rol, id, timestamp, desfaseSegundos } =
        registroMasReciente;

      try {
        // Validar datos del registro
        if (!id || !rol || !modoRegistro) {
          const error = `Datos faltantes en registro: ID=${id}, Rol=${rol}, Modo=${modoRegistro}`;
          console.warn(`⚠️  ${error}`);
          errores.push(error);
          registrosIgnorados++;
          continue;
        }

        // Obtener configuración de tabla
        const config = obtenerConfiguracionTabla(rol, modoRegistro);
        if (!config) {
          const error = `Configuración no encontrada para rol: ${rol}, modo: ${modoRegistro}`;
          console.warn(`⚠️  ${error}`);
          errores.push(error);
          registrosIgnorados++;
          continue;
        }

        const { tabla, campoID, campoJSON } = config;

        // Registrar con valores personalizados
        const resultado = await registrarAsistenciaConValoresPersonalizados(
          tabla,
          campoID,
          id, // Para roles normales, usar ID directamente
          mesActual,
          diaActual,
          campoJSON,
          {
            Timestamp: timestamp,
            DesfaseSegundos: desfaseSegundos,
          },
          rol,
          false // No es directivo
        );

        if (resultado.exito) {
          if (modoRegistro === ModoRegistro.Entrada) {
            registrosEntradaGuardados++;
          } else {
            registrosSalidaGuardados++;
          }
          console.log(`✅ ${resultado.mensaje}`);
        } else {
          console.log(`ℹ️  ${resultado.mensaje}`);
          registrosIgnorados++;
        }

        if (grupoRegistros.length > 1) {
          console.log(
            `🔄 Se procesaron ${grupoRegistros.length} registros duplicados para ${id}-${modoRegistro}, se tomó el más reciente`
          );
        }
      } catch (error) {
        const mensajeError = `Error al procesar registro para ID ${id}: ${
          error instanceof Error ? error.message : String(error)
        }`;
        console.error(`❌ ${mensajeError}`);
        errores.push(mensajeError);
        registrosIgnorados++;
      }
    }

    // 🆕 PROCESAR REGISTROS DE DIRECTIVOS (sin conversión - ya vienen con Id_Directivo)
    for (const [clave, grupoRegistros] of registrosAgrupadosDirectivos) {
      const registroMasReciente = grupoRegistros.reduce((max, current) =>
        current.timestamp > max.timestamp ? current : max
      );

      const {
        modoRegistro,
        rol,
        id,
        timestamp,
        desfaseSegundos,
        idDirectivo,
      } = registroMasReciente;

      try {
        console.log(
          `🏢 Procesando directivo con Id_Directivo: ${idDirectivo} (valor id: ${id})`
        );

        // Validar que tenemos el Id_Directivo
        if (!idDirectivo) {
          const error = `Id_Directivo faltante para registro: ${id}`;
          console.error(`❌ ${error}`);
          errores.push(error);
          registrosIgnorados++;
          continue;
        }

        // Obtener configuración de tabla para directivos
        const config = obtenerConfiguracionTabla(rol, modoRegistro);
        if (!config) {
          const error = `Configuración no encontrada para directivo: ${rol}, modo: ${modoRegistro}`;
          console.warn(`⚠️  ${error}`);
          errores.push(error);
          registrosIgnorados++;
          continue;
        }

        const { tabla, campoID, campoJSON } = config;

        // Registrar usando Id_Directivo directamente (sin conversión)
        const resultado = await registrarAsistenciaConValoresPersonalizados(
          tabla,
          campoID,
          idDirectivo, // 🆕 Usar Id_Directivo directamente
          mesActual,
          diaActual,
          campoJSON,
          {
            Timestamp: timestamp,
            DesfaseSegundos: desfaseSegundos,
          },
          rol,
          true // Es directivo
        );

        if (resultado.exito) {
          if (modoRegistro === ModoRegistro.Entrada) {
            registrosEntradaGuardados++;
          } else {
            registrosSalidaGuardados++;
          }
          console.log(`✅ Directivo: ${resultado.mensaje}`);
        } else {
          console.log(`ℹ️  Directivo: ${resultado.mensaje}`);
          registrosIgnorados++;
        }

        if (grupoRegistros.length > 1) {
          console.log(
            `🔄 Se procesaron ${grupoRegistros.length} registros duplicados de directivo para Id_Directivo ${idDirectivo}, se tomó el más reciente`
          );
        }
      } catch (error) {
        const mensajeError = `Error al procesar registro de directivo para Id_Directivo ${idDirectivo}: ${
          error instanceof Error ? error.message : String(error)
        }`;
        console.error(`❌ ${mensajeError}`);
        errores.push(mensajeError);
        registrosIgnorados++;
      }
    }

    console.log(
      "\n=== 📊 Resumen de persistencia de registros de personal ==="
    );
    console.log(
      `📥 Registros de entrada guardados: ${registrosEntradaGuardados}`
    );
    console.log(
      `📤 Registros de salida guardados: ${registrosSalidaGuardados}`
    );
    console.log(`⏭️  Registros ignorados: ${registrosIgnorados}`);

    if (errores.length > 0) {
      console.log(`❌ Errores encontrados: ${errores.length}`);
      errores.slice(0, 5).forEach((error) => console.log(`   - ${error}`));
      if (errores.length > 5) {
        console.log(`   ... y ${errores.length - 5} errores más`);
      }
    }

    return {
      registrosEntradaGuardados,
      registrosSalidaGuardados,
      registrosIgnorados,
      errores,
    };
  } catch (error) {
    const mensajeError = `Error general al persistir registros: ${
      error instanceof Error ? error.message : String(error)
    }`;
    console.error(`❌ ${mensajeError}`);
    throw new Error(mensajeError);
  }
}
