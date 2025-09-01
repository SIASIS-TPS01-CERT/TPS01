import { redisClient } from "../../../../config/Redis/RedisClient";
import { EXPRESION_REGULAR_PARA_IDENTIFICADORES_SIASIS } from "../../../../constants/REGEXP";
import { TipoAsistencia } from "../../../../interfaces/shared/AsistenciaRequests";
import { RolesSistema } from "../../../../interfaces/shared/RolesSistema";
import { obtenerFechasActuales } from "../../../utils/dates/obtenerFechasActuales";

// Interfaz para el tipo de registro que retorna la función
export interface RegistroPersonalRedis {
  fecha: string;
  modoRegistro: string;
  rol: string;
  id: string; // Para la mayoría de roles será ID, para directivos será Id_Directivo como string
  timestamp: number;
  desfaseSegundos: number;
  claveRedis: string;
  // 🆕 NUEVO CAMPO para identificar si es un directivo
  esDirectivo?: boolean;
  idDirectivo?: number; // Solo para directivos, convierte el ID string a number
}

/**
 * Obtiene los registros de asistencia del personal almacenados en Redis
 * para la fecha actual
 * 🆕 ACTUALIZADA para manejar directivos que usan Id_Directivo en lugar de ID
 */
export async function obtenerRegistrosAsistenciaPersonalRedis(): Promise<
  RegistroPersonalRedis[]
> {
  try {
    console.log(
      "🔍 Obteniendo registros de asistencia de personal desde Redis..."
    );

    // Obtener la fecha actual en Perú en formato YYYY-MM-DD
    const { fechaLocalPeru } = obtenerFechasActuales();
    const fechaFormateada = fechaLocalPeru.toISOString().split("T")[0];

    // Utilizamos el cliente Redis específico para registros de personal
    const redisClientePersonal = redisClient(TipoAsistencia.ParaPersonal);

    // Buscamos todas las claves que corresponden a la fecha actual
    const patron = `${fechaFormateada}:*`;
    const claves = await redisClientePersonal.keys(patron);

    console.log(
      `📊 Se encontraron ${claves.length} registros en Redis para la fecha ${fechaFormateada}`
    );

    // Si no hay claves, retornar array vacío
    if (claves.length === 0) {
      console.log("ℹ️  No se encontraron registros en Redis para procesar");
      return [];
    }

    // Obtenemos los valores para cada clave encontrada
    const registros: RegistroPersonalRedis[] = [];
    let registrosInvalidos = 0;
    let registrosDirectivos = 0;
    let registrosOtrosRoles = 0;

    for (const clave of claves) {
      try {
        const valor = await redisClientePersonal.get(clave);

        if (!valor) {
          console.warn(`⚠️  Valor vacío para la clave: ${clave}`);
          registrosInvalidos++;
          continue;
        }

        // Parsear la clave para extraer información
        // Formato esperado: YYYY-MM-DD:ModoRegistro:Actor:ID_o_ID
        const partesClave = clave.split(":");

        if (partesClave.length < 4) {
          console.warn(
            `⚠️  Formato de clave inválido: ${clave} (esperado: fecha:modo:actor:ID_o_id)`
          );
          registrosInvalidos++;
          continue;
        }

        const [fecha, modoRegistro, rol, identificador] = partesClave;

        // 🆕 LÓGICA ESPECIAL PARA DIRECTIVOS
        const esDirectivo = rol === RolesSistema.Directivo;
        let idParaRegistro = identificador;
        let idDirectivo: number | undefined;

        if (esDirectivo) {
          // Para directivos, el identificador es el Id_Directivo (número)
          const idDirectivoNum = parseInt(identificador);

          if (isNaN(idDirectivoNum) || idDirectivoNum <= 0) {
            console.warn(
              `⚠️  Id_Directivo inválido en clave ${clave}: ${identificador}`
            );
            registrosInvalidos++;
            continue;
          }

          idDirectivo = idDirectivoNum;
          idParaRegistro = identificador; // Mantenemos como string para compatibilidad
          registrosDirectivos++;

          console.log(`🏢 Directivo encontrado: Id_Directivo = ${idDirectivo}`);
        } else {
          // Para otros roles, validar que sea un ID válido (8 dígitos)
          if (!EXPRESION_REGULAR_PARA_IDENTIFICADORES_SIASIS.test(identificador)) {
            console.warn(
              `⚠️  ID inválido en clave ${clave}: ${identificador}`
            );
            registrosInvalidos++;
            continue;
          }
          registrosOtrosRoles++;
        }

        // Para personal, el valor es un array [timestamp, desfaseSegundos]
        if (!Array.isArray(valor) || valor.length < 2) {
          console.warn(
            `⚠️  Formato de valor inválido para clave ${clave}:`,
            valor
          );
          registrosInvalidos++;
          continue;
        }

        const timestamp = Number(valor[0]);
        const desfaseSegundos = Number(valor[1]);

        // Validar que sean números válidos
        if (isNaN(timestamp) || isNaN(desfaseSegundos)) {
          console.warn(
            `⚠️  Datos numéricos inválidos en clave ${clave}:`,
            valor
          );
          registrosInvalidos++;
          continue;
        }

        // Validar que el timestamp sea razonable (no muy antiguo ni futuro)
        const ahora = Date.now();
        const diferenciaDias =
          Math.abs(ahora - timestamp) / (1000 * 60 * 60 * 24);

        if (diferenciaDias > 2) {
          // Más de 2 días de diferencia
          console.warn(
            `⚠️  Timestamp sospechoso en clave ${clave}: ${new Date(
              timestamp
            ).toISOString()}`
          );
          // No marcamos como inválido, pero lo reportamos
        }

        registros.push({
          fecha,
          modoRegistro,
          rol,
          id: idParaRegistro, // Para directivos será Id_Directivo como string
          timestamp,
          desfaseSegundos,
          claveRedis: clave,
          // 🆕 CAMPOS ADICIONALES PARA DIRECTIVOS
          esDirectivo,
          idDirectivo,
        });
      } catch (error) {
        console.error(`❌ Error al procesar clave ${clave}:`, error);
        registrosInvalidos++;
      }
    }

    console.log(
      `✅ Se procesaron ${registros.length} registros válidos de personal`
    );
    console.log(`🏢 Registros de directivos: ${registrosDirectivos}`);
    console.log(`👥 Registros de otros roles: ${registrosOtrosRoles}`);

    if (registrosInvalidos > 0) {
      console.warn(
        `⚠️  Se encontraron ${registrosInvalidos} registros inválidos que fueron ignorados`
      );
    }

    // Mostrar ejemplos de registros encontrados para debugging
    if (registros.length > 0) {
      console.log(`🔍 Ejemplos de registros procesados:`);

      // Mostrar ejemplo de directivo si existe
      const ejemploDirectivo = registros.find((r) => r.esDirectivo);
      if (ejemploDirectivo) {
        console.log(
          `🏢 Directivo: Id=${ejemploDirectivo.idDirectivo}, Rol=${ejemploDirectivo.rol}, Modo=${ejemploDirectivo.modoRegistro}`
        );
      }

      // Mostrar ejemplos de otros roles
      const ejemplosOtros = registros.filter((r) => !r.esDirectivo).slice(0, 2);
      ejemplosOtros.forEach((r) => {
        console.log(
          `👥 ${r.rol}: ID=${r.id}, Modo=${
            r.modoRegistro
          }, Timestamp=${new Date(r.timestamp).toLocaleString()}`
        );
      });
    }

    return registros;
  } catch (error) {
    console.error(
      "❌ Error al obtener registros de asistencia desde Redis:",
      error
    );
    throw new Error(
      `Error al obtener registros de Redis: ${
        error instanceof Error ? error.message : String(error)
      }`
    );
  }
}
