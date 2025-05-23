import { redisClient } from "../../../../config/Redis/RedisClient";
import { TipoAsistencia } from "../../../../interfaces/shared/AsistenciaRequests";
import { obtenerFechasActuales } from "../../../utils/dates/obtenerFechasActuales";

// Interfaz para el tipo de registro que retorna la función
export interface RegistroPersonalRedis {
  fecha: string;
  modoRegistro: string;
  rol: string;
  dni: string;
  timestamp: number;
  desfaseSegundos: number;
  claveRedis: string;
}

/**
 * Obtiene los registros de asistencia del personal almacenados en Redis
 * para la fecha actual
 */
export async function obtenerRegistrosAsistenciaPersonalRedis(): Promise<RegistroPersonalRedis[]> {
  try {
    console.log("🔍 Obteniendo registros de asistencia de personal desde Redis...");
    
    // Obtener la fecha actual en Perú en formato YYYY-MM-DD
    const { fechaLocalPeru } = obtenerFechasActuales();
    const fechaFormateada = fechaLocalPeru.toISOString().split('T')[0];
    
    // Utilizamos el cliente Redis específico para registros de personal
    const redisClientePersonal = redisClient(TipoAsistencia.ParaPersonal);
    
    // Buscamos todas las claves que corresponden a la fecha actual
    // El patrón busca todas las claves que empiecen con la fecha actual
    const patron = `${fechaFormateada}:*`;
    const claves = await redisClientePersonal.keys(patron);
    
    console.log(`📊 Se encontraron ${claves.length} registros en Redis para la fecha ${fechaFormateada}`);
    
    // Si no hay claves, retornar array vacío
    if (claves.length === 0) {
      console.log("ℹ️  No se encontraron registros en Redis para procesar");
      return [];
    }
    
    // Obtenemos los valores para cada clave encontrada
    const registros: RegistroPersonalRedis[] = [];
    let registrosInvalidos = 0;
    
    for (const clave of claves) {
      try {
        const valor = await redisClientePersonal.get(clave);
        
        if (!valor) {
          console.warn(`⚠️  Valor vacío para la clave: ${clave}`);
          registrosInvalidos++;
          continue;
        }
        
        // Parsear la clave para extraer información
        // Formato esperado: YYYY-MM-DD:ModoRegistro:Actor:DNI
        const partesClave = clave.split(':');
        
        if (partesClave.length < 4) {
          console.warn(`⚠️  Formato de clave inválido: ${clave} (esperado: fecha:modo:actor:dni)`);
          registrosInvalidos++;
          continue;
        }
        
        const [fecha, modoRegistro, rol, dni] = partesClave;
        
        // Validar que el DNI tenga el formato correcto (8 dígitos)
        if (!/^\d{8}$/.test(dni)) {
          console.warn(`⚠️  DNI inválido en clave ${clave}: ${dni}`);
          registrosInvalidos++;
          continue;
        }
        
        // Para personal, el valor es un array [timestamp, desfaseSegundos]
        if (!Array.isArray(valor) || valor.length < 2) {
          console.warn(`⚠️  Formato de valor inválido para clave ${clave}:`, valor);
          registrosInvalidos++;
          continue;
        }
        
        const timestamp = Number(valor[0]);
        const desfaseSegundos = Number(valor[1]);
        
        // Validar que sean números válidos
        if (isNaN(timestamp) || isNaN(desfaseSegundos)) {
          console.warn(`⚠️  Datos numéricos inválidos en clave ${clave}:`, valor);
          registrosInvalidos++;
          continue;
        }
        
        // Validar que el timestamp sea razonable (no muy antiguo ni futuro)
        const ahora = Date.now();
        const diferenciaDias = Math.abs(ahora - timestamp) / (1000 * 60 * 60 * 24);
        
        if (diferenciaDias > 2) { // Más de 2 días de diferencia
          console.warn(`⚠️  Timestamp sospechoso en clave ${clave}: ${new Date(timestamp).toISOString()}`);
          // No marcamos como inválido, pero lo reportamos
        }
        
        registros.push({
          fecha,
          modoRegistro,
          rol,
          dni,
          timestamp,
          desfaseSegundos,
          claveRedis: clave
        });
        
      } catch (error) {
        console.error(`❌ Error al procesar clave ${clave}:`, error);
        registrosInvalidos++;
      }
    }
    
    console.log(`✅ Se procesaron ${registros.length} registros válidos de personal`);
    if (registrosInvalidos > 0) {
      console.warn(`⚠️  Se encontraron ${registrosInvalidos} registros inválidos que fueron ignorados`);
    }
    
    // Mostrar ejemplos de registros encontrados para debugging
    if (registros.length > 0) {
      console.log(`🔍 Ejemplos de registros procesados:`, registros.slice(0, 3).map(r => ({
        dni: r.dni,
        rol: r.rol,
        modo: r.modoRegistro,
        timestamp: new Date(r.timestamp).toLocaleString(),
        desfase: r.desfaseSegundos + 's'
      })));
    }
    
    return registros;
    
  } catch (error) {
    console.error("❌ Error al obtener registros de asistencia desde Redis:", error);
    throw new Error(`Error al obtener registros de Redis: ${error instanceof Error ? error.message : String(error)}`);
  }
}