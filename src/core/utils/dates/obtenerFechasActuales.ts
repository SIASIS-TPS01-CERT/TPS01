import { FECHA_HORA_MOCKEADAS } from "../../../constants/FECHA_HORA_MOCKEADAS";
import { ENTORNO } from "../../../constants/ENTORNO";
import { Entorno } from "../../../interfaces/shared/Entornos";
import { generarFechaHoraMockeada } from "./mock";

/**
 * Obtiene la fecha actual del sistema o la fecha mockeada si está habilitada
 * @returns Objeto con fechaUTC y fechaLocalPeru
 */
export function obtenerFechasActuales() {
  // Si el mock está habilitado, usamos la fecha mockeada
  const fechaUTC =
    ENTORNO === Entorno.LOCAL && FECHA_HORA_MOCKEADAS
      ? generarFechaHoraMockeada(2025, 5, 19, 9, 30, 0)
      : new Date();

  // Para la fecha local de Perú, simplemente restamos 5 horas a la fecha UTC
  // pero creamos un nuevo objeto para no modificar la fecha UTC original
  const fechaLocalPeru = new Date(fechaUTC);
  
  // UTC-5 para Perú
  // Usamos getUTCHours para obtener la hora en UTC sin considerar la zona horaria local
  const horaUTC = fechaUTC.getUTCHours();
  const nuevaHoraPeru = horaUTC - 5; // Convertir a hora Perú (UTC-5)
  
  // Si la nueva hora es negativa, ajustamos el día
  if (nuevaHoraPeru < 0) {
    // Obtenemos el día anterior
    fechaLocalPeru.setUTCDate(fechaLocalPeru.getUTCDate() - 1);
    // Ajustamos la hora (24 + nuevaHora)
    fechaLocalPeru.setUTCHours(24 + nuevaHoraPeru);
  } else {
    // Si la hora no es negativa, simplemente ajustamos la hora
    fechaLocalPeru.setUTCHours(nuevaHoraPeru);
  }

  return { fechaUTC, fechaLocalPeru };
}