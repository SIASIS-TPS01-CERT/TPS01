/**
 * Intenta extraer una hora formateada de varias formas posibles
 * @param valor - El valor a procesar (puede ser cualquier tipo)
 * @returns String en formato HH:MM:SS o null si no se puede extraer
 */
export function extraerHora(valor: any): string | null {
  console.log("Intentando extraer hora de:", valor, typeof valor);

  // Si es null o undefined, no podemos extraer nada
  if (valor === null || valor === undefined) {
    return null;
  }

  try {
    // Si es una cadena, intentamos procesarla de diferentes formas
    if (typeof valor === "string") {
      // Intento 1: Puede ser una cadena en formato ISO (contiene T o Z)
      if (valor.includes("T") || valor.includes("Z")) {
        const date = new Date(valor);
        if (!isNaN(date.getTime())) {
          return `${date.getHours().toString().padStart(2, "0")}:${date
            .getMinutes()
            .toString()
            .padStart(2, "0")}:${date
            .getSeconds()
            .toString()
            .padStart(2, "0")}`;
        }
      }

      // Intento 2: Puede ser una cadena en formato HH:MM:SS
      const timeRegex = /^(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?$/;
      if (timeRegex.test(valor)) {
        const parts = valor.split(":");
        return `${parts[0].padStart(2, "0")}:${parts[1].padStart(2, "0")}:${(
          parts[2] || "00"
        ).padStart(2, "0")}`;
      }

      // Intento 3: Si es una hora militar (solo dígitos y longitud 4 o 6)
      const militaryRegex = /^(\d{2})(\d{2})(\d{2})?$/;
      if (/^\d{4,6}$/.test(valor)) {
        const match = valor.match(militaryRegex);
        if (match) {
          return `${match[1]}:${match[2]}:${match[3] || "00"}`;
        }
      }
    }

    // Si es un objeto Date
    if (valor instanceof Date && !isNaN(valor.getTime())) {
      return `${valor.getHours().toString().padStart(2, "0")}:${valor
        .getMinutes()
        .toString()
        .padStart(2, "0")}:${valor.getSeconds().toString().padStart(2, "0")}`;
    }

    // Intento final: Convertir a Date y ver si funciona
    const date = new Date(valor);
    if (!isNaN(date.getTime())) {
      return `${date.getHours().toString().padStart(2, "0")}:${date
        .getMinutes()
        .toString()
        .padStart(2, "0")}:${date.getSeconds().toString().padStart(2, "0")}`;
    }
  } catch (error) {
    console.error("Error extrayendo hora:", error);
  }

  return null;
}

/**
 * Crea una fecha ISO válida combinando una fecha base con una hora
 * @param fechaBase Fecha base (para obtener año, mes, día)
 * @param horaString Hora en formato HH:MM:SS
 * @returns Objeto Date con la fecha y hora combinadas
 */
export function crearFechaConHora(fechaBase: Date, horaString: string | null): Date {
  try {
    if (!horaString) {
      throw new Error("Hora no proporcionada");
    }

    // Extraer fecha en formato YYYY-MM-DD
    const fechaString = fechaBase.toISOString().split("T")[0];

    // Crear nueva fecha combinando la fecha con la hora
    const fecha = new Date(`${fechaString}T${horaString}.000Z`);

    // Verificar que sea válida
    if (isNaN(fecha.getTime())) {
      throw new Error(
        `Fecha inválida generada: ${fechaString}T${horaString}.000Z`
      );
    }

    return fecha;
  } catch (error) {
    console.error("Error creando fecha con hora:", error);

    // Usar fecha base con hora 00:00:00 como fallback
    const fallback = new Date(fechaBase);
    fallback.setHours(0, 0, 0, 0);
    return fallback;
  }
}
