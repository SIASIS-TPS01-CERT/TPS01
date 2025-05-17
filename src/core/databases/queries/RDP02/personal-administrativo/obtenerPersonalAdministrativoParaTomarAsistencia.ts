import { PersonalAdministrativoParaTomaDeAsistencia } from "../../../../../interfaces/shared/Asistencia/DatosAsistenciaHoyIE20935";
import { verificarDentroSemanaGestion } from "../../../../utils/verificators/verificarDentroSemanaGestion";
import { verificarDentroVacacionesInterescolares } from "../../../../utils/verificators/verificarDentroVacacionesInterescolares";
import RDP02_DB_INSTANCES from '../../../connectors/postgres';

export async function obtenerPersonalAdministrativoParaTomarAsistencia(
  fechaActual: Date,
  vacacionesInterescolares: any[],
  semanaGestion: any | null
): Promise<PersonalAdministrativoParaTomaDeAsistencia[]> {
  // Verificar si estamos en vacaciones interescolares o semana de gestión
  const enVacacionesInterescolares = verificarDentroVacacionesInterescolares(
    fechaActual,
    vacacionesInterescolares
  );

  const enSemanaGestion = verificarDentroSemanaGestion(
    fechaActual,
    semanaGestion
  );

  // Obtener los horarios especiales si estamos en un periodo especial
  let horaInicio: Date | null = null;
  let horaFin: Date | null = null;

  if (enVacacionesInterescolares) {
    // Consulta para obtener los horarios de vacaciones interescolares
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

    // Extraer los valores
    for (const row of result.rows) {
      if (row.Nombre === 'Inicio_Horario_Laboral_Para_Personal_General_Vacaciones_Interescolares') {
        horaInicio = new Date(row.Valor);
      } else if (row.Nombre === 'Fin_Horario_Laboral_Para_Personal_General_Vacaciones_Interescolares') {
        horaFin = new Date(row.Valor);
      }
    }
  } else if (enSemanaGestion) {
    // Consulta para obtener los horarios de semana de gestión
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

    // Extraer los valores
    for (const row of result.rows) {
      if (row.Nombre === 'Inicio_Horario_Laboral_Para_Personal_General_Semana_Gestion') {
        horaInicio = new Date(row.Valor);
      } else if (row.Nombre === 'Fin_Horario_Laboral_Para_Personal_General_Semana_Gestion') {
        horaFin = new Date(row.Valor);
      }
    }
  }

  // Consultar el personal administrativo
  const sql = `
    SELECT 
      "DNI_Personal_Administrativo", 
      "Genero", 
      "Nombres", 
      "Apellidos", 
      "Cargo", 
      "Google_Drive_Foto_ID", 
      "Horario_Laboral_Entrada", 
      "Horario_Laboral_Salida"
    FROM "T_Personal_Administrativo"
    WHERE "Estado" = true
  `;

  const result = await RDP02_DB_INSTANCES.query(sql);

  // Obtener la fecha actual en Perú (UTC-5)
  const fechaUTC = fechaActual || new Date();
  // Restar 5 horas para convertir a hora peruana
  const fechaPeruana = new Date(fechaUTC.getTime() - 5 * 60 * 60 * 1000);
  const fechaPeruanaString = fechaPeruana.toISOString().split("T")[0];

  // Procesar los resultados para combinar fecha y hora
  const datosConFechas = result.rows.map((row: any) => {
    let fechaHoraEntrada, fechaHoraSalida;

    // Si estamos en un periodo especial, usamos los horarios especiales
    if ((enVacacionesInterescolares || enSemanaGestion) && horaInicio && horaFin) {
      // Crear nuevas fechas con la fecha actual y las horas especiales
      const entradaEspecial = new Date(fechaPeruana);
      entradaEspecial.setHours(
        horaInicio.getHours(),
        horaInicio.getMinutes(),
        horaInicio.getSeconds(),
        0
      );

      const salidaEspecial = new Date(fechaPeruana);
      salidaEspecial.setHours(
        horaFin.getHours(),
        horaFin.getMinutes(),
        horaFin.getSeconds(),
        0
      );

      fechaHoraEntrada = entradaEspecial.toISOString();
      fechaHoraSalida = salidaEspecial.toISOString();
    } else {
      // Usar los horarios normales de la base de datos
      const horaEntrada = row.Horario_Laboral_Entrada;
      const horaSalida = row.Horario_Laboral_Salida;

      // Crear fechas combinando la fecha peruana con las horas
      // Al usar new Date() con formato ISO, JavaScript asume UTC
      const fechaEntrada = new Date(`${fechaPeruanaString}T${horaEntrada}`);
      const fechaSalida = new Date(`${fechaPeruanaString}T${horaSalida}`);

      // Convertir a formato ISO con Z (UTC)
      fechaHoraEntrada = fechaEntrada.toISOString();
      fechaHoraSalida = fechaSalida.toISOString();
    }

    // Devolver el objeto con los campos actualizados
    return {
      ...row,
      Hora_Entrada_Dia_Actual: fechaHoraEntrada,
      Hora_Salida_Dia_Actual: fechaHoraSalida,
    };
  });

  return datosConFechas;
}