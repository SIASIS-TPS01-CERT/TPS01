import { Fecha_ISO_8601 } from "../../../../interfaces/shared/Fechas";

/**
 * Formatea una fecha al formato DD-MM-YYYY usado en el archivo de IDs
 */
export function formatearFechaParaArchivoIds(fecha: Date): Fecha_ISO_8601 {
  const dia = fecha.getUTCDate().toString().padStart(2, "0");
  const mes = (fecha.getUTCMonth() + 1).toString().padStart(2, "0");
  const anio = fecha.getUTCFullYear();

  return `${dia}-${mes}-${anio}`;
}

/**
 * Convierte un string en formato DD-MM-YYYY a objeto Date
 */
export function parsearFechaDesdeArchivoIds(fechaStr: string): Date {
  const [dia, mes, anio] = fechaStr.split("-").map(Number);
  return new Date(Date.UTC(anio, mes - 1, dia));
}
