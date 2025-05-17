// src/core/utils/dates.ts
export function obtenerFechasActuales() {
  const fechaUTC = new Date();
  
  // Crear fecha en zona horaria de Perú (UTC-5)
  const fechaLocalPeru = new Date(fechaUTC);
  fechaLocalPeru.setHours(fechaLocalPeru.getHours() - 5);
  
  return { fechaUTC, fechaLocalPeru };
}

