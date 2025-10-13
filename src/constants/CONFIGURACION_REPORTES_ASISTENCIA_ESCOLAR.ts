// src/constants/REPORTES_CONFIG.ts

/**
 * Configuración para el sistema de reportes de asistencia
 */

// ============================================================
// MODO DE ENVÍO DE CORREOS
// ============================================================

/**
 * Si es TRUE: Envía un correo con todos los destinatarios en copia
 * - Directivos: Un correo para todos
 * - Auxiliares: Un correo para todos
 *
 * Si es FALSE: Envía correos individualizados a cada persona
 * - Cada directivo recibe su propio correo
 * - Cada auxiliar recibe su propio correo
 *
 * NOTA: Tutores y Profesores de Primaria SIEMPRE reciben correos individuales
 */
export const ENVIAR_CORREOS_EN_GRUPO = false;

/**
 * Si es TRUE: Tutores (secundaria) y Profesores (primaria) reciben el Excel completo
 * con todas las aulas (multihoja), y deben buscar su salón
 *
 * Si es FALSE: Cada tutor/profesor recibe solo el Excel de su aula (1 hoja)
 *
 * NOTA: Este ajuste solo aplica cuando ENVIAR_CORREOS_EN_GRUPO = false
 */
export const ENVIAR_EXCEL_COMPLETO_A_TUTORES_Y_PROFESORES = false;

// ============================================================
// CONFIGURACIÓN DE ARCHIVOS EXCEL
// ============================================================

/**
 * Nombre de la hoja cuando el Excel contiene solo un aula
 */
export const NOMBRE_HOJA_UNICA = "Reporte de Asistencia";

/**
 * Prefijo para nombres de hojas en Excel multihoja
 * Ejemplo: "Grado 1", "Grado 2", etc.
 */
export const PREFIJO_NOMBRE_HOJA = "Grado";

// ============================================================
// CONFIGURACIÓN DE CORREOS
// ============================================================

/**
 * Asunto base para correos de reportes de faltas
 */
export const ASUNTO_REPORTE_FALTAS =
  "📊 Reporte de Faltas Consecutivas - I.E. 20935 Asunción 8";

/**
 * Asunto base para correos de reportes de tardanzas
 */
export const ASUNTO_REPORTE_TARDANZAS =
  "📊 Reporte de Tardanzas Consecutivas - I.E. 20935 Asunción 8";

/**
 * Nombre del remitente para correos
 */
export const NOMBRE_REMITENTE = "Sistema de Asistencia I.E. 20935";

// ============================================================
// TIMEOUT Y REINTENTOS
// ============================================================

/**
 * Tiempo de espera entre envíos de correos (en milisegundos)
 * Para evitar límites de rate limiting del servidor SMTP
 */
export const DELAY_ENTRE_CORREOS_MS = 1000; // 1 segundo

/**
 * Número máximo de reintentos al enviar un correo
 */
export const MAX_REINTENTOS_ENVIO_CORREO = 3;

/**
 * Tiempo de espera antes de reintentar (en milisegundos)
 */
export const DELAY_REINTENTO_MS = 5000; // 5 segundos
