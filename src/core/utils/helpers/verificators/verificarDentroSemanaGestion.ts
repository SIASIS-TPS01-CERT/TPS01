import { RangoFechas } from "../../../../interfaces/shared/Asistencia/DatosAsistenciaHoyIE20935";

export function verificarDentroSemanaGestion(
  fechaActual: Date,
  semanaGestion: RangoFechas | null
): false | RangoFechas {
  // Si semanaGestion es null, significa que no hay semana de gestión configurada
  if (!semanaGestion) {
    console.log("No hay semana de gestión configurada");
    return false;
  }

  try {
    // Obtener componentes de fecha en UTC para evitar problemas de zona horaria
    const actualAnio = fechaActual.getUTCFullYear();
    const actualMes = fechaActual.getUTCMonth();
    const actualDia = fechaActual.getUTCDate();

    // Obtener componentes de las fechas de inicio y fin en UTC
    let inicioAnio, inicioMes, inicioDia;
    let finAnio, finMes, finDia;

    // Manejar tanto objetos Date como posibles strings
    if (typeof semanaGestion.Inicio === "string") {
      const fechaInicio = new Date(semanaGestion.Inicio);
      inicioAnio = fechaInicio.getUTCFullYear();
      inicioMes = fechaInicio.getUTCMonth();
      inicioDia = fechaInicio.getUTCDate();
    } else {
      inicioAnio = semanaGestion.Inicio.getUTCFullYear();
      inicioMes = semanaGestion.Inicio.getUTCMonth();
      inicioDia = semanaGestion.Inicio.getUTCDate();
    }

    if (typeof semanaGestion.Fin === "string") {
      const fechaFin = new Date(semanaGestion.Fin);
      finAnio = fechaFin.getUTCFullYear();
      finMes = fechaFin.getUTCMonth();
      finDia = fechaFin.getUTCDate();
    } else {
      finAnio = semanaGestion.Fin.getUTCFullYear();
      finMes = semanaGestion.Fin.getUTCMonth();
      finDia = semanaGestion.Fin.getUTCDate();
    }

    console.log(
      `Verificando si fecha ${fechaActual.toISOString()} (${actualAnio}-${
        actualMes + 1
      }-${actualDia}) está en semana de gestión`
    );
    console.log(
      `Semana de gestión: ${inicioAnio}-${
        inicioMes + 1
      }-${inicioDia} hasta ${finAnio}-${finMes + 1}-${finDia}`
    );

    // Crear timestamps para comparación precisa
    const inicioTimestamp = Date.UTC(inicioAnio, inicioMes, inicioDia);
    const finTimestamp = Date.UTC(finAnio, finMes, finDia);
    const actualTimestamp = Date.UTC(actualAnio, actualMes, actualDia);

    // Verificar si la fecha actual está dentro del rango
    if (actualTimestamp >= inicioTimestamp && actualTimestamp <= finTimestamp) {
      console.log(
        `¡Fecha ${actualAnio}-${
          actualMes + 1
        }-${actualDia} dentro de la semana de gestión!`
      );
      return semanaGestion;
    }

    console.log(
      `Fecha ${actualAnio}-${
        actualMes + 1
      }-${actualDia} NO está en la semana de gestión`
    );
    return false;
  } catch (error) {
    console.error("Error al verificar semana de gestión:", error, {
      fechaActual: fechaActual?.toISOString(),
      semanaGestion: JSON.stringify(semanaGestion),
    });
    return false;
  }
}
