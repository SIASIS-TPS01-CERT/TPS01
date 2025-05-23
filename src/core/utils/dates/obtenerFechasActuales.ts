import { ENTORNO } from "../../../constants/ENTORNO";
import { FECHA_HORA_MOCKEADAS } from "../../../constants/FECHA_HORA_MOCKEADAS";
import { Entorno } from "../../../interfaces/shared/Entornos";
import { generarFechaHoraMockeada } from "./mock";

export function obtenerFechasActuales() {
  // Si el mock está habilitado, usamos la fecha mockeada
  const fechaUTC =
    ENTORNO === Entorno.LOCAL && FECHA_HORA_MOCKEADAS
      ? generarFechaHoraMockeada(2025, 4, 18, 9, 30, 0) // 12:30 UTC
      : new Date();

  // Para la fecha local de Perú (UTC-5)
  const fechaLocalPeru = new Date(fechaUTC);

  // CORRECCIÓN: Para UTC-5, RESTAMOS 5 horas al timestamp
  // No manipulamos componentes individuales
  fechaLocalPeru.setTime(fechaUTC.getTime() - 5 * 60 * 60 * 1000);

  return { fechaUTC, fechaLocalPeru };
}
