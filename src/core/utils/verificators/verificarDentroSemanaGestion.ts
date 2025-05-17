import { RangoFechas } from "../../../interfaces/shared/Asistencia/DatosAsistenciaHoyIE20935";

export function verificarDentroSemanaGestion(
  fechaActual: Date,
  semanaGestion: RangoFechas | null
): false | RangoFechas {
  // Si semanaGestion es null, significa que no hay semana de gestión configurada
  if (!semanaGestion) {
    return false;
  }

  // Normalizar las fechas para comparar solo año, mes y día
  const actual = new Date(
    fechaActual.getFullYear(),
    fechaActual.getMonth(),
    fechaActual.getDate()
  );

  const inicio = new Date(
    semanaGestion.Inicio.getFullYear(),
    semanaGestion.Inicio.getMonth(),
    semanaGestion.Inicio.getDate()
  );

  const fin = new Date(
    semanaGestion.Fin.getFullYear(),
    semanaGestion.Fin.getMonth(),
    semanaGestion.Fin.getDate()
  );

  // Verificar si la fecha actual está dentro del rango
  if (actual >= inicio && actual <= fin) {
    return semanaGestion;
  }

  return false;
}
