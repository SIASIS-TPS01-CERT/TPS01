import { T_Vacaciones_Interescolares } from "@prisma/client";

export function verificarDentroVacacionesInterescolares(
  fechaActual: Date,
  vacacionesInterescolares: T_Vacaciones_Interescolares[]
): boolean {
  // Obtener año, mes y día en UTC para evitar problemas de zona horaria
  const actualAnio = fechaActual.getUTCFullYear();
  const actualMes = fechaActual.getUTCMonth();
  const actualDia = fechaActual.getUTCDate();
  
  console.log(`Verificando si fecha ${fechaActual.toISOString()} (${actualAnio}-${actualMes+1}-${actualDia}) está en vacaciones interescolares`);
  
  // Si no hay vacaciones definidas, retornar false
  if (!vacacionesInterescolares || !Array.isArray(vacacionesInterescolares) || vacacionesInterescolares.length === 0) {
    console.log("No hay períodos de vacaciones interescolares definidos");
    return false;
  }

  // Verificar si la fecha actual está dentro de alguno de los períodos de vacaciones
  for (const vacacion of vacacionesInterescolares) {
    try {
      let fechaInicio: Date;
      let fechaFin: Date;
      
      // Convertir a Date si es string o usar directamente si es Date
      if (typeof vacacion.Fecha_Inicio === 'string') {
        fechaInicio = new Date(vacacion.Fecha_Inicio);
      } else {
        fechaInicio = vacacion.Fecha_Inicio;
      }
      
      if (typeof vacacion.Fecha_Conclusion === 'string') {
        fechaFin = new Date(vacacion.Fecha_Conclusion);
      } else {
        fechaFin = vacacion.Fecha_Conclusion;
      }
      
      // Extraer año, mes y día en UTC para evitar problemas de zona horaria
      const inicioAnio = fechaInicio.getUTCFullYear();
      const inicioMes = fechaInicio.getUTCMonth();
      const inicioDia = fechaInicio.getUTCDate();
      
      const finAnio = fechaFin.getUTCFullYear();
      const finMes = fechaFin.getUTCMonth();
      const finDia = fechaFin.getUTCDate();
      
      console.log(`Período de vacaciones: ${inicioAnio}-${inicioMes+1}-${inicioDia} hasta ${finAnio}-${finMes+1}-${finDia}`);
      
      // Crear fechas UTC para comparación precisa
      // Usar Date.UTC para crear timestamps en UTC directamente
      const inicioTimestamp = Date.UTC(inicioAnio, inicioMes, inicioDia);
      const finTimestamp = Date.UTC(finAnio, finMes, finDia);
      const actualTimestamp = Date.UTC(actualAnio, actualMes, actualDia);
      
      // Comparar usando los valores numéricos de las fechas (timestamps)
      if (
        actualTimestamp >= inicioTimestamp && 
        actualTimestamp <= finTimestamp
      ) {
        console.log(`¡Fecha ${actualAnio}-${actualMes+1}-${actualDia} dentro del período de vacaciones interescolares!`);
        return true;
      }
    } catch (error) {
      console.error("Error al procesar período de vacaciones:", error, {
        vacacion: JSON.stringify(vacacion)
      });
    }
  }
  
  console.log(`Fecha ${actualAnio}-${actualMes+1}-${actualDia} NO está en ningún período de vacaciones interescolares`);
  return false;
}