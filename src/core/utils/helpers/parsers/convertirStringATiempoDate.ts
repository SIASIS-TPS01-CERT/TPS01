// Función para convertir string de tiempo (HH:MM:SS) a objeto Date
export function convertirStringTiempoADate(
  fechaBase: Date,
  tiempoStr: string
): Date {
  const [horas, minutos, segundos] = tiempoStr.split(":").map(Number);

  const fechaResultado = new Date(fechaBase);
  fechaResultado.setHours(horas, minutos, segundos, 0);

  return fechaResultado;
}
