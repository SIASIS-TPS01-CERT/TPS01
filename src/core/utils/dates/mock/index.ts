/**
 * Genera una fecha y hora mockeada para pruebas
 * @param year - Año (ej: 2025)
 * @param month - Mes (1-12)
 * @param day - Día (1-31)
 * @param hours - Horas (0-23)
 * @param minutes - Minutos (0-59)
 * @param seconds - Segundos (0-59)
 * @param milliseconds - Milisegundos (0-999)
 * @returns Un objeto Date con la fecha especificada
 */
export function generarFechaHoraMockeada(
  year: number,
  month: number,
  day: number,
  hours: number = 0,
  minutes: number = 0,
  seconds: number = 0,
  milliseconds: number = 0
): Date {
  // En JavaScript, los meses son 0-indexed (0-11)
  return new Date(
    Date.UTC(year, month - 1, day, hours, minutes, seconds, milliseconds)
  );
}
