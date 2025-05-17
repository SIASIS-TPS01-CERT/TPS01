// src/core/utils/verificators/verificarDentroVacacionesInterescolares.ts
export function verificarDentroVacacionesInterescolares(
  fechaActual: Date,
  vacacionesInterescolares: any[]
): boolean {
  // Normalizar la fecha actual para comparar solo año, mes y día
  const actual = new Date(
    fechaActual.getFullYear(),
    fechaActual.getMonth(),
    fechaActual.getDate()
  );

  // Verificar si la fecha actual está dentro de alguno de los períodos de vacaciones
  for (const vacacion of vacacionesInterescolares) {
    const inicio = new Date(
      vacacion.Fecha_Inicio.getFullYear(),
      vacacion.Fecha_Inicio.getMonth(),
      vacacion.Fecha_Inicio.getDate()
    );

    const fin = new Date(
      vacacion.Fecha_Conclusion.getFullYear(),
      vacacion.Fecha_Conclusion.getMonth(),
      vacacion.Fecha_Conclusion.getDate()
    );

    if (actual >= inicio && actual <= fin) {
      return true;
    }
  }

  return false;
}
