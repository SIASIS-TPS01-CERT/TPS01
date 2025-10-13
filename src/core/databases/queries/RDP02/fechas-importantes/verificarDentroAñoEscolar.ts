import { RangoFechas } from "../../../../../interfaces/shared/Asistencia/DatosAsistenciaHoyIE20935";

export default function verificarFueraAñoEscolar(
  fechaActual: Date,
  fechaInicioAñoEscolar: Date | string,
  fechaFinAñoEscolar: Date | string
): false | RangoFechas {
  try {
    // Convertir fechas a objetos Date si son strings
    const inicioAñoEscolar =
      typeof fechaInicioAñoEscolar === "string"
        ? new Date(fechaInicioAñoEscolar)
        : fechaInicioAñoEscolar;

    const finAñoEscolar =
      typeof fechaFinAñoEscolar === "string"
        ? new Date(fechaFinAñoEscolar)
        : fechaFinAñoEscolar;

    // Obtener componentes de fecha en UTC para evitar problemas de zona horaria
    const actualAnio = fechaActual.getUTCFullYear();
    const actualMes = fechaActual.getUTCMonth();
    const actualDia = fechaActual.getUTCDate();

    const inicioAnio = inicioAñoEscolar.getUTCFullYear();
    const inicioMes = inicioAñoEscolar.getUTCMonth();
    const inicioDia = inicioAñoEscolar.getUTCDate();

    const finAnio = finAñoEscolar.getUTCFullYear();
    const finMes = finAñoEscolar.getUTCMonth();
    const finDia = finAñoEscolar.getUTCDate();

    console.log(
      `Verificando si fecha ${fechaActual.toISOString()} (${actualAnio}-${
        actualMes + 1
      }-${actualDia}) está fuera del año escolar`
    );
    console.log(
      `Año escolar: ${inicioAnio}-${
        inicioMes + 1
      }-${inicioDia} hasta ${finAnio}-${finMes + 1}-${finDia}`
    );

    // Crear timestamps en UTC para comparación precisa
    const inicioTimestamp = Date.UTC(inicioAnio, inicioMes, inicioDia);
    const finTimestamp = Date.UTC(finAnio, finMes, finDia);
    const actualTimestamp = Date.UTC(actualAnio, actualMes, actualDia);

    // Verificar si la fecha actual está fuera del rango del año escolar
    const estaFuera =
      actualTimestamp < inicioTimestamp || actualTimestamp > finTimestamp;

    // Si está fuera, devolver el rango para indicar el período del año escolar
    if (estaFuera) {
      console.log(
        `¡Fecha ${actualAnio}-${
          actualMes + 1
        }-${actualDia} está FUERA del año escolar!`
      );
      return {
        Inicio: inicioAñoEscolar,
        Fin: finAñoEscolar,
      };
    }

    console.log(
      `Fecha ${actualAnio}-${
        actualMes + 1
      }-${actualDia} está DENTRO del año escolar`
    );
    return false;
  } catch (error) {
    console.error("Error al verificar fuera del año escolar:", error, {
      fechaActual: fechaActual?.toISOString(),
      fechaInicioAñoEscolar:
        typeof fechaInicioAñoEscolar === "string"
          ? fechaInicioAñoEscolar
          : fechaInicioAñoEscolar?.toISOString(),
      fechaFinAñoEscolar:
        typeof fechaFinAñoEscolar === "string"
          ? fechaFinAñoEscolar
          : fechaFinAñoEscolar?.toISOString(),
    });

    // En caso de error, asumir que está dentro del año escolar (devolver false)
    return false;
  }
}
