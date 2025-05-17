import { RangoFechas } from "../../../interfaces/shared/Asistencia/DatosAsistenciaHoyIE20935";

export default function verificarFueraAñoEscolar(
  fechaActual: Date,
  fechaInicioAñoEscolar: Date,
  fechaFinAñoEscolar: Date
): false | RangoFechas {
  // Verificar si la fecha actual está dentro del rango del año escolar
  const estaFuera =
    fechaActual < fechaInicioAñoEscolar || fechaActual > fechaFinAñoEscolar;

  // Si está fuera, devolver el rango para indicar el período del año escolar
  if (estaFuera) {
    return {
      Inicio: fechaInicioAñoEscolar,
      Fin: fechaFinAñoEscolar,
    };
  }

  // Si no está fuera, devolver false
  return false;
}
