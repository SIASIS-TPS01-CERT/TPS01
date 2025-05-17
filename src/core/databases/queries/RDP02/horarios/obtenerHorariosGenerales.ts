import { HorarioTomaAsistencia } from "../../../../../interfaces/shared/Asistencia/DatosAsistenciaHoyIE20935";
import { verificarDentroSemanaGestion } from "../../../../utils/verificators/verificarDentroSemanaGestion";
import { verificarDentroVacacionesInterescolares } from "../../../../utils/verificators/verificarDentroVacacionesInterescolares";
import RDP02_DB_INSTANCES from "../../../connectors/postgres";

type HorariosGeneralesReturn = {
  TomaAsistenciaRangoTotalPersonales: HorarioTomaAsistencia;
  TomaAsistenciaProfesorPrimaria: HorarioTomaAsistencia;
  TomaAsistenciaAuxiliares: HorarioTomaAsistencia;
};

export async function obtenerHorariosGenerales(
  fechaActual: Date,
  vacacionesInterescolares: any[],
  semanaGestion: any | null
): Promise<HorariosGeneralesReturn> {
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

  // Si estamos en vacaciones interescolares
  if (enVacacionesInterescolares) {
    // Consulta para obtener los horarios específicos de vacaciones interescolares
    const sql = `
      SELECT 
        "Nombre", "Valor"
      FROM 
        "T_Horarios_Asistencia"
      WHERE 
        "Nombre" IN (
          'Inicio_Horario_Laboral_Para_Personal_General_Vacaciones_Interescolares',
          'Fin_Horario_Laboral_Para_Personal_General_Vacaciones_Interescolares'
        )
    `;

    const result = await RDP02_DB_INSTANCES.query(sql);

    // Verificamos si encontramos exactamente los dos registros
    if (result.rows.length !== 2) {
      throw new Error(`No se encontraron los horarios completos para vacaciones interescolares. Se esperaban 2 registros y se encontraron ${result.rows.length}`);
    }

    // Extraer los valores
    let horaInicio: Date | null = null;
    let horaFin: Date | null = null;

    for (const row of result.rows) {
      if (row.Nombre === 'Inicio_Horario_Laboral_Para_Personal_General_Vacaciones_Interescolares') {
        horaInicio = new Date(row.Valor);
      } else if (row.Nombre === 'Fin_Horario_Laboral_Para_Personal_General_Vacaciones_Interescolares') {
        horaFin = new Date(row.Valor);
      }
    }

    // Verificar que obtuvimos ambos valores
    if (!horaInicio || !horaFin) {
      throw new Error("No se pudieron identificar correctamente los horarios para vacaciones interescolares");
    }

    // Devolvemos los mismos horarios para todos los tipos de personal
    return {
      TomaAsistenciaRangoTotalPersonales: {
        Inicio: horaInicio,
        Fin: horaFin,
      },
      TomaAsistenciaProfesorPrimaria: {
        Inicio: horaInicio,
        Fin: horaFin,
      },
      TomaAsistenciaAuxiliares: {
        Inicio: horaInicio,
        Fin: horaFin,
      },
    };
  }
  
  // Si estamos en semana de gestión
  else if (enSemanaGestion) {
    // Consulta para obtener los horarios específicos de semana de gestión
    const sql = `
      SELECT 
        "Nombre", "Valor"
      FROM 
        "T_Horarios_Asistencia"
      WHERE 
        "Nombre" IN (
          'Inicio_Horario_Laboral_Para_Personal_General_Semana_Gestion',
          'Fin_Horario_Laboral_Para_Personal_General_Semana_Gestion'
        )
    `;

    const result = await RDP02_DB_INSTANCES.query(sql);

    // Verificamos si encontramos exactamente los dos registros
    if (result.rows.length !== 2) {
      throw new Error(`No se encontraron los horarios completos para semana de gestión. Se esperaban 2 registros y se encontraron ${result.rows.length}`);
    }

    // Extraer los valores
    let horaInicio: Date | null = null;
    let horaFin: Date | null = null;

    for (const row of result.rows) {
      if (row.Nombre === 'Inicio_Horario_Laboral_Para_Personal_General_Semana_Gestion') {
        horaInicio = new Date(row.Valor);
      } else if (row.Nombre === 'Fin_Horario_Laboral_Para_Personal_General_Semana_Gestion') {
        horaFin = new Date(row.Valor);
      }
    }

    // Verificar que obtuvimos ambos valores
    if (!horaInicio || !horaFin) {
      throw new Error("No se pudieron identificar correctamente los horarios para semana de gestión");
    }

    // Devolvemos los mismos horarios para todos los tipos de personal
    return {
      TomaAsistenciaRangoTotalPersonales: {
        Inicio: horaInicio,
        Fin: horaFin,
      },
      TomaAsistenciaProfesorPrimaria: {
        Inicio: horaInicio,
        Fin: horaFin,
      },
      TomaAsistenciaAuxiliares: {
        Inicio: horaInicio,
        Fin: horaFin,
      },
    };
  }

  // Si no estamos en periodo especial, obtenemos los horarios normales
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
        'Horario_Laboral_Rango_Total_Inicio',
        'Horario_Laboral_Rango_Total_Fin'
      )
  `;

  const result = await RDP02_DB_INSTANCES.query(sql);

  // Convertir a un objeto para fácil acceso
  const horarios: Record<string, Date> = {};
  for (const row of result.rows) {
    horarios[row.Nombre] = new Date(row.Valor);
  }

  // Verificar que obtuvimos todos los horarios necesarios
  const requiredHorarios = [
    'Hora_Inicio_Asistencia_Primaria',
    'Hora_Final_Asistencia_Primaria',
    'Hora_Inicio_Asistencia_Secundaria',
    'Hora_Final_Asistencia_Secundaria',
    'Horario_Laboral_Rango_Total_Inicio',
    'Horario_Laboral_Rango_Total_Fin'
  ];

  for (const horario of requiredHorarios) {
    if (!horarios[horario]) {
      throw new Error(`No se encontró el horario: ${horario}`);
    }
  }

  return {
    TomaAsistenciaRangoTotalPersonales: {
      Inicio: horarios["Horario_Laboral_Rango_Total_Inicio"],
      Fin: horarios["Horario_Laboral_Rango_Total_Fin"],
    },
    TomaAsistenciaProfesorPrimaria: {
      Inicio: horarios["Hora_Inicio_Asistencia_Primaria"],
      Fin: horarios["Hora_Final_Asistencia_Primaria"],
    },
    TomaAsistenciaAuxiliares: {
      Inicio: horarios["Hora_Inicio_Asistencia_Secundaria"],
      Fin: horarios["Hora_Final_Asistencia_Secundaria"],
    },
  };
}