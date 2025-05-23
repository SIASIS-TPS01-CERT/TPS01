import { T_Vacaciones_Interescolares } from "@prisma/client";
import { HorarioTomaAsistencia } from "../../../../../interfaces/shared/Asistencia/DatosAsistenciaHoyIE20935";
import {
  crearFechaConHora,
  extraerHora,
} from "../../../../utils/dates/modificacionFechas";
import { verificarDentroSemanaGestion } from "../../../../utils/verificators/verificarDentroSemanaGestion";
import { verificarDentroVacacionesInterescolares } from "../../../../utils/verificators/verificarDentroVacacionesInterescolares";
import RDP02_DB_INSTANCES from "../../../connectors/postgres";

// Constantes para la extensión del rango total
const EXTENSION_INICIO_TOMA_ASISTENCIA_RANGO_TOTAL_HORAS = 1;
const EXTENSION_FIN_TOMA_ASISTENCIA_RANGO_TOTAL_HORAS = 1;

type HorariosGeneralesReturn = {
  TomaAsistenciaRangoTotalPersonales: HorarioTomaAsistencia;
  TomaAsistenciaProfesorPrimaria: HorarioTomaAsistencia;
  TomaAsistenciaAuxiliares: HorarioTomaAsistencia;
};

/**
 * Aplica extensión de horario al rango total (extiende 1 hora antes y 1 hora después)
 */
function aplicarExtensionRangoTotal(horaInicio: Date, horaFin: Date): { inicio: Date; fin: Date } {
  const inicioExtendido = new Date(horaInicio);
  inicioExtendido.setUTCHours(horaInicio.getUTCHours() - EXTENSION_INICIO_TOMA_ASISTENCIA_RANGO_TOTAL_HORAS);
  
  const finExtendido = new Date(horaFin);
  finExtendido.setUTCHours(horaFin.getUTCHours() + EXTENSION_FIN_TOMA_ASISTENCIA_RANGO_TOTAL_HORAS);
  
  console.log(`🔄 Extensión aplicada al rango total:`);
  console.log(`   Original: ${horaInicio.toISOString()} - ${horaFin.toISOString()}`);
  console.log(`   Extendido: ${inicioExtendido.toISOString()} - ${finExtendido.toISOString()}`);
  
  return {
    inicio: inicioExtendido,
    fin: finExtendido
  };
}

export async function obtenerHorariosGenerales(
  fechaActual: Date,
  vacacionesInterescolares: T_Vacaciones_Interescolares[],
  semanaGestion: any | null
): Promise<HorariosGeneralesReturn> {
  try {
    console.log("==========================================");
    console.log("ANÁLISIS DE FECHAS EN obtenerHorariosGenerales");
    console.log(`Fecha recibida: ${fechaActual?.toISOString()}`);
    console.log(`  -> Día UTC: ${fechaActual?.getUTCDate()}`);
    console.log(`  -> Mes UTC: ${fechaActual?.getUTCMonth() + 1}`);
    console.log(`  -> Año UTC: ${fechaActual?.getUTCFullYear()}`);
    console.log(
      `  -> Hora UTC: ${fechaActual?.getUTCHours()}:${fechaActual?.getUTCMinutes()}`
    );
    console.log("==========================================");

    // Asegurar que fechaActual sea válida
    if (!fechaActual || isNaN(fechaActual.getTime())) {
      console.error("Fecha inválida proporcionada:", fechaActual);
      fechaActual = new Date(); // Usar fecha actual como fallback
    }

    // Primero verificamos si estamos en vacaciones interescolares
    const enVacacionesInterescolares = verificarDentroVacacionesInterescolares(
      fechaActual,
      vacacionesInterescolares
    );

    // Verificamos si estamos en semana de gestión
    const enSemanaGestion = verificarDentroSemanaGestion(
      fechaActual,
      semanaGestion
    );

    console.log("Estado de períodos especiales:", {
      enVacacionesInterescolares,
      enSemanaGestion,
    });

    // Si estamos en vacaciones interescolares o semana de gestión
    if (enVacacionesInterescolares || enSemanaGestion) {
      // Determinamos el período específico para la consulta
      let nombresHorarios = [];

      if (enVacacionesInterescolares) {
        nombresHorarios = [
          "Inicio_Horario_Laboral_Para_Personal_General_Vacaciones_Interescolares",
          "Fin_Horario_Laboral_Para_Personal_General_Vacaciones_Interescolares",
        ];
        console.log("Consultando horarios para VACACIONES INTERESCOLARES");
      } else {
        nombresHorarios = [
          "Inicio_Horario_Laboral_Para_Personal_General_Semana_Gestion",
          "Fin_Horario_Laboral_Para_Personal_General_Semana_Gestion",
        ];
        console.log("Consultando horarios para SEMANA DE GESTIÓN");
      }

      // Consulta para obtener los horarios específicos para este período
      const sql = `
        SELECT 
          "Nombre", "Valor"
        FROM 
          "T_Horarios_Asistencia"
        WHERE 
          "Nombre" IN (
            '${nombresHorarios[0]}',
            '${nombresHorarios[1]}'
          )
      `;

      const result = await RDP02_DB_INSTANCES.query(sql);
      console.log(`Resultados de horarios para período especial:`, result.rows);

      // Extraer los valores
      let horaInicioStr: string | null = null;
      let horaFinStr: string | null = null;

      for (const row of result.rows) {
        if (row.Nombre === nombresHorarios[0]) {
          horaInicioStr = extraerHora(row.Valor);
          console.log(`Hora inicio extraída: ${horaInicioStr} de ${row.Valor}`);
        } else if (row.Nombre === nombresHorarios[1]) {
          horaFinStr = extraerHora(row.Valor);
          console.log(`Hora fin extraída: ${horaFinStr} de ${row.Valor}`);
        }
      }

      // Verificar que obtuvimos ambos valores
      if (!horaInicioStr || !horaFinStr) {
        console.error(
          "No se pudieron extraer ambos horarios para períodos especiales:",
          {
            inicio: horaInicioStr,
            fin: horaFinStr,
          }
        );

        // Valores predeterminados para períodos especiales
        horaInicioStr = "08:00:00";
        horaFinStr = "13:00:00"; // Horario reducido en períodos especiales
      }

      // Crear fechas combinando con la fecha actual
      const horaInicio = crearFechaConHora(fechaActual, horaInicioStr);
      const horaFin = crearFechaConHora(fechaActual, horaFinStr);

      console.log(`Horarios para período especial:`, {
        inicio: horaInicio.toISOString(),
        fin: horaFin.toISOString(),
      });

      // Aplicar extensión solo al rango total
      const rangoTotalExtendido = aplicarExtensionRangoTotal(horaInicio, horaFin);

      // Durante períodos especiales, todos los roles usan el mismo horario especial
      // PERO el rango total se extiende con las constantes
      return {
        TomaAsistenciaRangoTotalPersonales: {
          Inicio: rangoTotalExtendido.inicio,
          Fin: rangoTotalExtendido.fin,
        },
        TomaAsistenciaProfesorPrimaria: {
          Inicio: horaInicio, // Sin extensión
          Fin: horaFin,       // Sin extensión
        },
        TomaAsistenciaAuxiliares: {
          Inicio: horaInicio, // Sin extensión
          Fin: horaFin,       // Sin extensión
        },
      };
    }

    // Si no estamos en periodo especial, obtenemos los horarios normales con nombres estáticos
    const sql = `
      SELECT 
        "Nombre", "Valor"
      FROM 
        "T_Horarios_Asistencia"
      WHERE 
        "Nombre" IN (
          'Hora_Inicio_Asistencia_Primaria',
          'Hora_Final_Asistencia_Primaria',
          'Hora_Inicio_Asistencia_Secundaria',
          'Hora_Final_Asistencia_Secundaria',
          'Inicio_Horario_Laboral_Profesores_Primaria',
          'Fin_Horario_Laboral_Profesores_Primaria',
          'Inicio_Horario_Laboral_Auxiliar',
          'Fin_Horario_Laboral_Auxiliar',
          'Horario_Laboral_Rango_Total_Inicio',
          'Horario_Laboral_Rango_Total_Fin'
        )
    `;

    const result = await RDP02_DB_INSTANCES.query(sql);
    console.log("Resultados de horarios normales:", result.rows);

    // Inicializar objeto para almacenar los horarios extraídos
    const horariosExtraidos = {
      horaPrimaria: {
        inicio: null as string | null,
        fin: null as string | null,
      },
      horaSecundaria: {
        inicio: null as string | null,
        fin: null as string | null,
      },
      horaLaboralPrimaria: {
        inicio: null as string | null,
        fin: null as string | null,
      },
      horaLaboralAuxiliar: {
        inicio: null as string | null,
        fin: null as string | null,
      },
      horaLaboralTotal: {
        inicio: null as string | null,
        fin: null as string | null,
      },
    };

    // Extraer los valores por nombre específico
    for (const row of result.rows) {
      try {
        const horaStr = extraerHora(row.Valor);
        console.log(
          `Extrayendo hora para [${row.Nombre}]: ${horaStr} (de ${row.Valor})`
        );

        // Asignar según nombre exacto
        switch (row.Nombre) {
          case "Hora_Inicio_Asistencia_Primaria":
            horariosExtraidos.horaPrimaria.inicio = horaStr;
            break;
          case "Hora_Final_Asistencia_Primaria":
            horariosExtraidos.horaPrimaria.fin = horaStr;
            break;
          case "Hora_Inicio_Asistencia_Secundaria":
            horariosExtraidos.horaSecundaria.inicio = horaStr;
            break;
          case "Hora_Final_Asistencia_Secundaria":
            horariosExtraidos.horaSecundaria.fin = horaStr;
            break;
          case "Inicio_Horario_Laboral_Profesores_Primaria":
            horariosExtraidos.horaLaboralPrimaria.inicio = horaStr;
            break;
          case "Fin_Horario_Laboral_Profesores_Primaria":
            horariosExtraidos.horaLaboralPrimaria.fin = horaStr;
            break;
          case "Inicio_Horario_Laboral_Auxiliar":
            horariosExtraidos.horaLaboralAuxiliar.inicio = horaStr;
            break;
          case "Fin_Horario_Laboral_Auxiliar":
            horariosExtraidos.horaLaboralAuxiliar.fin = horaStr;
            break;
          case "Horario_Laboral_Rango_Total_Inicio":
            horariosExtraidos.horaLaboralTotal.inicio = horaStr;
            break;
          case "Horario_Laboral_Rango_Total_Fin":
            horariosExtraidos.horaLaboralTotal.fin = horaStr;
            break;
        }
      } catch (error) {
        console.error(`Error procesando hora [${row.Nombre}]:`, error, {
          raw: row.Valor,
        });
      }
    }

    // Aplicar valores predeterminados donde faltan horarios
    const valoresPredeterminados = {
      asistenciaPrimaria: { inicio: "07:45:00", fin: "12:45:00" },
      asistenciaSecundaria: { inicio: "13:00:00", fin: "18:30:00" },
      laboralPrimaria: { inicio: "07:45:00", fin: "12:45:00" },
      laboralAuxiliar: { inicio: "12:30:00", fin: "18:30:00" },
      laboralTotal: { inicio: "07:00:00", fin: "19:00:00" },
    };

    // Horarios para profesores primaria
    if (!horariosExtraidos.horaLaboralPrimaria.inicio) {
      horariosExtraidos.horaLaboralPrimaria.inicio =
        horariosExtraidos.horaPrimaria.inicio ||
        valoresPredeterminados.laboralPrimaria.inicio;
    }

    if (!horariosExtraidos.horaLaboralPrimaria.fin) {
      horariosExtraidos.horaLaboralPrimaria.fin =
        horariosExtraidos.horaPrimaria.fin ||
        valoresPredeterminados.laboralPrimaria.fin;
    }

    // Horarios para auxiliares
    if (!horariosExtraidos.horaLaboralAuxiliar.inicio) {
      horariosExtraidos.horaLaboralAuxiliar.inicio =
        horariosExtraidos.horaSecundaria.inicio ||
        valoresPredeterminados.laboralAuxiliar.inicio;
    }

    if (!horariosExtraidos.horaLaboralAuxiliar.fin) {
      horariosExtraidos.horaLaboralAuxiliar.fin =
        horariosExtraidos.horaSecundaria.fin ||
        valoresPredeterminados.laboralAuxiliar.fin;
    }

    // Horarios totales
    if (!horariosExtraidos.horaLaboralTotal.inicio) {
      horariosExtraidos.horaLaboralTotal.inicio =
        valoresPredeterminados.laboralTotal.inicio;
    }

    if (!horariosExtraidos.horaLaboralTotal.fin) {
      horariosExtraidos.horaLaboralTotal.fin =
        valoresPredeterminados.laboralTotal.fin;
    }

    // Crear objetos Date combinando con la fecha actual (SIN extensión primero)
    const horaInicioTotalBase = crearFechaConHora(
      fechaActual,
      horariosExtraidos.horaLaboralTotal.inicio
    );

    const horaFinTotalBase = crearFechaConHora(
      fechaActual,
      horariosExtraidos.horaLaboralTotal.fin
    );

    // Aplicar extensión solo al rango total
    const rangoTotalExtendido = aplicarExtensionRangoTotal(horaInicioTotalBase, horaFinTotalBase);

    const horaInicioPrimaria = crearFechaConHora(
      fechaActual,
      horariosExtraidos.horaLaboralPrimaria.inicio
    );

    const horaFinPrimaria = crearFechaConHora(
      fechaActual,
      horariosExtraidos.horaLaboralPrimaria.fin
    );

    const horaInicioAuxiliar = crearFechaConHora(
      fechaActual,
      horariosExtraidos.horaLaboralAuxiliar.inicio
    );

    const horaFinAuxiliar = crearFechaConHora(
      fechaActual,
      horariosExtraidos.horaLaboralAuxiliar.fin
    );

    console.log("Fechas combinadas generadas (horario normal):");
    console.log(
      `- Total Base: ${horaInicioTotalBase.toISOString()} - ${horaFinTotalBase.toISOString()}`
    );
    console.log(
      `- Total Extendido: ${rangoTotalExtendido.inicio.toISOString()} - ${rangoTotalExtendido.fin.toISOString()}`
    );
    console.log(
      `- Primaria: ${horaInicioPrimaria.toISOString()} - ${horaFinPrimaria.toISOString()}`
    );
    console.log(
      `- Auxiliares: ${horaInicioAuxiliar.toISOString()} - ${horaFinAuxiliar.toISOString()}`
    );

    return {
      TomaAsistenciaRangoTotalPersonales: {
        Inicio: rangoTotalExtendido.inicio,
        Fin: rangoTotalExtendido.fin,
      },
      TomaAsistenciaProfesorPrimaria: {
        Inicio: horaInicioPrimaria, // Sin extensión
        Fin: horaFinPrimaria,       // Sin extensión
      },
      TomaAsistenciaAuxiliares: {
        Inicio: horaInicioAuxiliar, // Sin extensión
        Fin: horaFinAuxiliar,       // Sin extensión
      },
    };
  } catch (error) {
    console.error("Error general en obtenerHorariosGenerales:", error);

    // Crear horarios predeterminados en caso de error
    const initDate = new Date(fechaActual);
    initDate.setUTCHours(8, 0, 0, 0);

    const endDate = new Date(fechaActual);
    endDate.setUTCHours(16, 0, 0, 0);

    // Aplicar extensión también en caso de error
    const rangoTotalExtendido = aplicarExtensionRangoTotal(initDate, endDate);

    return {
      TomaAsistenciaRangoTotalPersonales: {
        Inicio: rangoTotalExtendido.inicio,
        Fin: rangoTotalExtendido.fin,
      },
      TomaAsistenciaProfesorPrimaria: {
        Inicio: initDate,  // Sin extensión
        Fin: endDate,      // Sin extensión
      },
      TomaAsistenciaAuxiliares: {
        Inicio: initDate,  // Sin extensión
        Fin: endDate,      // Sin extensión
      },
    };
  }
}