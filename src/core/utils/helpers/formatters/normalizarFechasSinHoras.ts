/**
 * Normaliza una fecha estableciendo la hora a medianoche (00:00:00.000)
 * para permitir comparaciones precisas solo por fecha
 */
export function normalizarFecha(fecha: Date): Date {
  const fechaNormalizada = new Date(fecha);
  fechaNormalizada.setHours(0, 0, 0, 0);
  return fechaNormalizada;
}
