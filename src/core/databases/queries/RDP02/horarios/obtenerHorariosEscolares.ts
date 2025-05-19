import { HorarioTomaAsistencia } from "../../../../../interfaces/shared/Asistencia/DatosAsistenciaHoyIE20935";
import { NivelEducativo } from "../../../../../interfaces/shared/NivelEducativo";
import {
  crearFechaConHora,
  extraerHora,
} from "../../../../utils/dates/modificacionFechas";

import RDP02_DB_INSTANCES from "../../../connectors/postgres";

export async function obtenerHorariosEscolares(
  fechaActual: Date,
  
): Promise<Record<NivelEducativo, HorarioTomaAsistencia>> {
  console.log(
    "obtenerHorariosEscolares - Utilizando fecha:",
    fechaActual?.toISOString()
  );

  // Asegurar que fechaActual sea válida
  if (!fechaActual || isNaN(fechaActual.getTime())) {
    console.error("Fecha inválida proporcionada:", fechaActual);
    fechaActual = new Date(); // Usar fecha actual como fallback
  }

  try {
    const sql = `
      SELECT "Nombre", "Valor", "Descripcion"
      FROM "T_Horarios_Asistencia"
      WHERE "Nombre" IN (
        'Hora_Inicio_Asistencia_Primaria',
        'Hora_Final_Asistencia_Primaria',
        'Hora_Inicio_Asistencia_Secundaria',
        'Hora_Final_Asistencia_Secundaria'
      )
    `;

    const result = await RDP02_DB_INSTANCES.query(sql);
    console.log("Resultados de horarios escolares:", result.rows);

    // Extraer las horas como strings
    const horariosStr: Record<string, string | null> = {};

    for (const row of result.rows) {
      try {
        const horaStr = extraerHora(row.Valor);
        console.log(
          `Extrayendo hora para [${row.Nombre}]: ${horaStr} (de ${row.Valor})`
        );
        horariosStr[row.Nombre] = horaStr;
      } catch (error) {
        console.error(`Error procesando hora [${row.Nombre}]:`, error, {
          raw: row.Valor,
        });
      }
    }

    // Verificar que tenemos todas las horas necesarias
    const horasRequeridas = [
      "Hora_Inicio_Asistencia_Primaria",
      "Hora_Final_Asistencia_Primaria",
      "Hora_Inicio_Asistencia_Secundaria",
      "Hora_Final_Asistencia_Secundaria",
    ];

    // Proporcionar valores predeterminados para horas faltantes
    for (const hora of horasRequeridas) {
      if (!horariosStr[hora]) {
        console.warn(`Hora faltante: ${hora}, usando valor predeterminado`);

        if (hora.includes("Inicio")) {
          if (hora.includes("Primaria")) {
            horariosStr[hora] = "07:45:00";
          } else {
            horariosStr[hora] = "13:00:00";
          }
        } else if (hora.includes("Final")) {
          if (hora.includes("Primaria")) {
            horariosStr[hora] = "12:45:00";
          } else {
            horariosStr[hora] = "18:30:00";
          }
        }
      }
    }

    // Crear objetos Date a partir de las horas extraídas y la fecha actual
    const horaInicioPrimaria = crearFechaConHora(
      fechaActual,
      horariosStr["Hora_Inicio_Asistencia_Primaria"]
    );
    const horaFinPrimaria = crearFechaConHora(
      fechaActual,
      horariosStr["Hora_Final_Asistencia_Primaria"]
    );
    const horaInicioSecundaria = crearFechaConHora(
      fechaActual,
      horariosStr["Hora_Inicio_Asistencia_Secundaria"]
    );
    const horaFinSecundaria = crearFechaConHora(
      fechaActual,
      horariosStr["Hora_Final_Asistencia_Secundaria"]
    );

    console.log("Horarios escolares generados:");
    console.log(
      `- Primaria: ${horaInicioPrimaria.toISOString()} - ${horaFinPrimaria.toISOString()}`
    );
    console.log(
      `- Secundaria: ${horaInicioSecundaria.toISOString()} - ${horaFinSecundaria.toISOString()}`
    );

    return {
      [NivelEducativo.PRIMARIA]: {
        Inicio: horaInicioPrimaria,
        Fin: horaFinPrimaria,
      },
      [NivelEducativo.SECUNDARIA]: {
        Inicio: horaInicioSecundaria,
        Fin: horaFinSecundaria,
      },
    };
  } catch (error) {
    console.error("Error en obtenerHorariosEscolares:", error);

    // Valores predeterminados en caso de error
    const inicioP = new Date(fechaActual);
    inicioP.setHours(7, 45, 0, 0);

    const finP = new Date(fechaActual);
    finP.setHours(12, 45, 0, 0);

    const inicioS = new Date(fechaActual);
    inicioS.setHours(13, 0, 0, 0);

    const finS = new Date(fechaActual);
    finS.setHours(18, 30, 0, 0);

    return {
      [NivelEducativo.PRIMARIA]: {
        Inicio: inicioP,
        Fin: finP,
      },
      [NivelEducativo.SECUNDARIA]: {
        Inicio: inicioS,
        Fin: finS,
      },
    };
  }
}
