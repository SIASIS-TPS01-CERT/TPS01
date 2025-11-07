import { T_Vacaciones_Interescolares } from "@prisma/client";
import { DURACION_HORA_ACADEMICA_EN_MINUTOS } from "../../../../../constants/DURACION_HORA_ACADEMICA_EN_MINUTOS";
import { ProfesorTutorSecundariaParaTomaDeAsistencia } from "../../../../../interfaces/shared/Asistencia/DatosAsistenciaHoyIE20935";
import { extraerHora } from "../../../../utils/dates/modificacionFechas";
import RDP02_DB_INSTANCES from "../../../connectors/postgres";
import { verificarDentroSemanaGestion } from "../fechas-importantes/verificarDentroSemanaGestion";
import { verificarDentroVacacionesInterescolares } from "../../../../utils/helpers/verificators/verificarDentroVacacionesInterescolares";

/**
 * Calcula el horario considerando los recreos
 * @param horaBaseDate - Hora base desde donde se calcula
 * @param indice - Índice del bloque (1-7)
 * @param bloqueInicioRecreo - Bloque después del cual comienza el recreo
 * @param duracionRecreoMinutos - Duración del recreo en minutos
 * @param duracionHoraAcademicaMinutos - Duración de cada bloque académico
 * @param esHoraSalida - Indica si estamos calculando hora de salida (fin de bloque)
 * @returns Date con la hora calculada
 */
function calcularHoraConRecreo(
  horaBaseDate: Date,
  indice: number,
  bloqueInicioRecreo: number,
  duracionRecreoMinutos: number,
  duracionHoraAcademicaMinutos: number,
  esHoraSalida: boolean = false
): Date {
  try {
    // Validar índice para asegurarnos que está dentro del rango permitido (1-7)
    const indiceValido = Math.max(1, Math.min(indice, 7));

    console.log(
      `Calculando hora con recreo: Índice ${indice} (válido: ${indiceValido}), bloque recreo: ${bloqueInicioRecreo}, duración recreo: ${duracionRecreoMinutos}min, duración hora: ${duracionHoraAcademicaMinutos}min, es salida: ${esHoraSalida}`
    );

    const resultado = new Date(horaBaseDate);

    // Determinar cuántos minutos adicionar considerando el recreo
    let minutosAdicionales = 0;

    if (indiceValido <= bloqueInicioRecreo) {
      // Para bloques antes o igual al bloque de recreo
      minutosAdicionales = (indiceValido - 1) * duracionHoraAcademicaMinutos;

      // Si es hora de salida, añadir la duración del bloque para obtener la hora de finalización
      if (esHoraSalida) {
        minutosAdicionales += duracionHoraAcademicaMinutos;
      }
    } else {
      // Para bloques después del recreo
      minutosAdicionales =
        (indiceValido - 1) * duracionHoraAcademicaMinutos +
        duracionRecreoMinutos;

      // Si es hora de salida, añadir la duración del bloque para obtener la hora de finalización
      if (esHoraSalida) {
        minutosAdicionales += duracionHoraAcademicaMinutos;
      }
    }

    // Extraer horas y minutos de la hora base
    const horaBase = horaBaseDate.getHours();
    const minutosBase = horaBaseDate.getMinutes();

    // Calcular nueva hora y minutos
    const totalMinutos = minutosBase + minutosAdicionales;
    const nuevosMinutos = totalMinutos % 60;
    const horasAdicionales = Math.floor(totalMinutos / 60);
    const nuevaHora = horaBase + horasAdicionales;

    // Configurar el resultado
    resultado.setHours(nuevaHora, nuevosMinutos, 0, 0);

    console.log(
      `Resultado de calcularHoraConRecreo: ${resultado.toISOString()}`
    );

    return resultado;
  } catch (error) {
    console.error("Error en calcularHoraConRecreo:", error);

    // En caso de error, devolver una fecha de fallback
    const fallback = new Date(horaBaseDate);
    if (esHoraSalida) {
      // Si es hora de salida, agregamos algunas horas como fallback
      fallback.setHours(fallback.getHours() + 5, 0, 0, 0);
    }
    return fallback;
  }
}

/**
 * Obtiene la lista de profesores de secundaria para tomar asistencia en una fecha específica
 * @param fecha - Fecha para la que se buscan los profesores
 * @param vacacionesInterescolares - Lista de periodos de vacaciones interescolares
 * @param semanaGestion - Datos de la semana de gestión
 * @param horariosEspeciales - Horarios especiales preconsultados (opcional)
 * @returns Lista de profesores con sus horarios calculados para el día
 */
export async function obtenerProfesoresSecundariaParaTomarAsistencia(
  fecha: Date,
  vacacionesInterescolares: T_Vacaciones_Interescolares[],
  semanaGestion: any | null,
  horariosEspeciales?: {
    inicio?: Date;
    fin?: Date;
    tipo?: string;
  }
): Promise<ProfesorTutorSecundariaParaTomaDeAsistencia[]> {
  try {
    console.log("==========================================");
    console.log(
      "obtenerProfesoresSecundariaParaTomarAsistencia - Fecha:",
      fecha?.toISOString()
    );
    console.log(
      "Vacaciones interescolares recibidas:",
      vacacionesInterescolares?.length
    );
    console.log(
      "Semana gestión recibida:",
      semanaGestion ? "Presente" : "No presente"
    );
    console.log("==========================================");

    // Verificar si estamos en vacaciones interescolares o semana de gestión
    const enVacacionesInterescolares = verificarDentroVacacionesInterescolares(
      fecha,
      vacacionesInterescolares
    );

    const enSemanaGestion = verificarDentroSemanaGestion(fecha, semanaGestion);

    console.log("Estado períodos especiales:", {
      enVacacionesInterescolares,
      enSemanaGestion,
    });

    // Obtener el día de la semana correctamente, considerando que la fecha puede estar en UTC
    // 1. Extraer componentes individuales como año, mes, día de la fecha UTC
    const añoUTC = fecha.getUTCFullYear();
    const mesUTC = fecha.getUTCMonth();
    const diaUTC = fecha.getUTCDate();

    // 2. Crear una nueva fecha local con estos componentes para eliminar cualquier problema de zona horaria
    const fechaLocal = new Date(añoUTC, mesUTC, diaUTC, 12, 0, 0); // Usamos mediodía para evitar problemas con cambios de día por zona horaria

    // 3. Obtener el día de la semana de esta fecha local
    const diaSemana = fechaLocal.getDay(); // 0-6, 0 siendo domingo

    // 4. Convertir para el formato BD (1-7, donde 1 es lunes y 7 es domingo)
    const diaSemanaDB = diaSemana === 0 ? 7 : diaSemana;

    console.log(`Fecha analizada: ${fechaLocal.toDateString()}`);
    console.log(`Día de la semana (0-6): ${diaSemana}`);
    console.log(`Día de la semana para BD (1-7): ${diaSemanaDB}`);

    // IMPORTANTE: Siempre obtenemos SOLO profesores que tienen cursos para ese día,
    // independientemente de si estamos en período especial o no
    // AHORA INCLUIMOS información del aula asignada
    const profesoresQuery = `
      SELECT 
        ps."Id_Profesor_Secundaria", 
        ps."Nombres", 
        ps."Apellidos", 
        ps."Genero", 
        ps."Google_Drive_Foto_ID",
        MIN(ch."Indice_Hora_Academica_Inicio") as "Indice_Entrada",
        MAX(ch."Indice_Hora_Academica_Inicio" + ch."Cant_Hora_Academicas") as "Indice_Salida",
        a."Id_Aula",
        a."Color",
        a."Grado",
        a."Nivel",
        a."Seccion"
      FROM "T_Profesores_Secundaria" ps
      JOIN "T_Cursos_Horario" ch ON ps."Id_Profesor_Secundaria" = ch."Id_Profesor_Secundaria"
      LEFT JOIN "T_Aulas" a ON ps."Id_Profesor_Secundaria" = a."Id_Profesor_Secundaria"
      WHERE ps."Estado" = true AND ch."Dia_Semana" = $1
      GROUP BY ps."Id_Profesor_Secundaria", ps."Nombres", ps."Apellidos", ps."Genero", ps."Google_Drive_Foto_ID", a."Id_Aula", a."Color", a."Grado", a."Nivel", a."Seccion"
    `;

    const profesoresResult = await RDP02_DB_INSTANCES.query(profesoresQuery, [
      diaSemanaDB,
    ]);
    console.log(
      `Encontrados ${profesoresResult.rows.length} profesores con cursos para el día ${diaSemanaDB}`
    );

    // Si no hay profesores con cursos este día, retornar array vacío
    if (profesoresResult.rows.length === 0) {
      console.log(
        "No se encontraron profesores con cursos para este día. Retornando array vacío."
      );
      return [];
    }

    // Si estamos en un periodo especial (vacaciones o semana de gestión)
    // Sobrescribimos los horarios, pero mantenemos la misma lista de profesores
    if (enVacacionesInterescolares || enSemanaGestion) {
      let horaInicioStr: string | null = null;
      let horaFinStr: string | null = null;

      // Determinar qué período estamos manejando
      const periodoTipo = enVacacionesInterescolares
        ? "Vacaciones_Interescolares"
        : "Semana_Gestion";

      // Consulta para obtener los horarios específicos
      const horariosEspecialesQuery = `
        SELECT 
          "Nombre", "Valor"
        FROM 
          "T_Horarios_Asistencia"
        WHERE 
          "Nombre" IN (
            'Inicio_Horario_Laboral_Para_Personal_General_${periodoTipo}',
            'Fin_Horario_Laboral_Para_Personal_General_${periodoTipo}'
          )
      `;

      const horariosEspecialesResult = await RDP02_DB_INSTANCES.query(
        horariosEspecialesQuery
      );
      console.log(
        `Resultados horarios especiales ${periodoTipo}:`,
        horariosEspecialesResult.rows
      );

      // Extraer los valores
      for (const row of horariosEspecialesResult.rows) {
        if (row.Nombre.includes("Inicio")) {
          horaInicioStr = extraerHora(row.Valor);
          console.log(`Hora inicio ${periodoTipo} extraída: ${horaInicioStr}`);
        } else if (row.Nombre.includes("Fin")) {
          horaFinStr = extraerHora(row.Valor);
          console.log(`Hora fin ${periodoTipo} extraída: ${horaFinStr}`);
        }
      }

      // Verificar que obtuvimos ambos valores
      if (!horaInicioStr || !horaFinStr) {
        console.error(
          `No se pudieron extraer los horarios para ${periodoTipo}`
        );

        // Valores predeterminados según documentación
        horaInicioStr = "08:00:00"; // 8:00 AM predeterminado para periodos especiales
        horaFinStr = "13:00:00"; // 1:00 PM predeterminado para periodos especiales
      }

      // Extraer solo la fecha (YYYY-MM-DD) de fecha
      const fechaString = fecha.toISOString().split("T")[0];

      // Crear fechas ISO combinando la fecha con las horas
      const horaEntradaISO = new Date(`${fechaString}T${horaInicioStr}.000Z`);
      const horaSalidaISO = new Date(`${fechaString}T${horaFinStr}.000Z`);

      console.log(
        `Horarios especiales generados para período ${periodoTipo}:`,
        {
          entrada: horaEntradaISO.toISOString(),
          salida: horaSalidaISO.toISOString(),
        }
      );

      // Asignar horarios especiales a los profesores con cursos este día
      console.log(
        `Aplicando horarios especiales de ${periodoTipo} a los profesores con cursos este día`
      );

      return profesoresResult.rows.map((profesor: any) => ({
        Id_Profesor_Secundaria: profesor.Id_Profesor_Secundaria,
        Nombres: profesor.Nombres,
        Apellidos: profesor.Apellidos,
        Genero: profesor.Genero,
        Google_Drive_Foto_ID: profesor.Google_Drive_Foto_ID,
        Hora_Entrada_Dia_Actual: horaEntradaISO,
        Hora_Salida_Dia_Actual: horaSalidaISO,
        Aula: profesor.Id_Aula
          ? {
              Id_Aula: profesor.Id_Aula,
              Color: profesor.Color,
              Grado: profesor.Grado,
              Nivel: profesor.Nivel,
              Seccion: profesor.Seccion,
            }
          : null,
        _periodoEspecial: periodoTipo,
      }));
    }

    // Si NO estamos en período especial, continuamos con la lógica normal
    // para calcular los horarios basados en los índices de entrada/salida

    // 1. Obtener los ajustes del sistema para el recreo
    const ajustesQuery = `
      SELECT "Nombre", "Valor" 
      FROM "T_Ajustes_Generales_Sistema" 
      WHERE "Nombre" IN ('BLOQUE_INICIO_RECREO_SECUNDARIA', 'DURACION_RECREO_SECUNDARIA_MINUTOS')
    `;

    const ajustesResult = await RDP02_DB_INSTANCES.query(ajustesQuery);
    console.log("Resultados ajustes sistema:", ajustesResult.rows);

    // Extraer valores de los ajustes
    const ajustes = ajustesResult.rows.reduce((acc: any, row: any) => {
      acc[row.Nombre] = parseInt(row.Valor, 10);
      return acc;
    }, {});

    const bloqueInicioRecreo = ajustes.BLOQUE_INICIO_RECREO_SECUNDARIA || 4;
    const duracionRecreoMinutos =
      ajustes.DURACION_RECREO_SECUNDARIA_MINUTOS || 15;

    console.log("Ajustes recreo:", {
      bloqueInicioRecreo,
      duracionRecreoMinutos,
    });

    // 2. Obtener la hora de inicio del horario escolar secundaria
    const horariosQuery = `
      SELECT "Valor" 
      FROM "T_Horarios_Asistencia" 
      WHERE "Nombre" = 'Hora_Inicio_Asistencia_Secundaria'
    `;

    const horariosResult = await RDP02_DB_INSTANCES.query(horariosQuery);
    console.log("Resultado hora inicio secundaria:", horariosResult.rows);

    // Extraer la hora de inicio
    const horaInicioValor = horariosResult.rows[0]?.Valor || "13:00:00";
    const horaInicioStr = extraerHora(horaInicioValor) || "13:00:00";
    console.log(
      `Hora inicio extraída: ${horaInicioStr} (de ${horaInicioValor})`
    );

    // Crear una fecha combinando la fecha base con la hora de inicio
    const fechaString = fecha.toISOString().split("T")[0];
    const horaInicio = new Date(`${fechaString}T${horaInicioStr}.000Z`);

    console.log(`Hora inicio base: ${horaInicio.toISOString()}`);

    // 3. Convertir resultados a objetos con horarios calculados
    return profesoresResult.rows.map((profesor: any) => {
      try {
        console.log(
          `Procesando horarios para profesor ${profesor.Id_Profesor_Secundaria}:`,
          {
            indiceEntrada: profesor.Indice_Entrada,
            indiceSalida: profesor.Indice_Salida,
          }
        );

        // Verificar que los índices sean válidos
        if (!profesor.Indice_Entrada || !profesor.Indice_Salida) {
          throw new Error("Índices de hora inválidos");
        }

        // Calcular hora de entrada
        const horaEntrada = calcularHoraConRecreo(
          horaInicio,
          profesor.Indice_Entrada,
          bloqueInicioRecreo,
          duracionRecreoMinutos,
          DURACION_HORA_ACADEMICA_EN_MINUTOS,
          false // No es hora de salida
        );

        // Utilizamos el enfoque de índice efectivo: el "Indice_Salida" de la BD
        // representa el índice del siguiente bloque después del último impartido,
        // por lo que restamos 1 para obtener el índice del último bloque efectivo
        const indiceSalidaEfectivo = profesor.Indice_Salida - 1;

        // Calculamos la hora de salida como el final de ese bloque efectivo
        const horaSalida = calcularHoraConRecreo(
          horaInicio,
          indiceSalidaEfectivo,
          bloqueInicioRecreo,
          duracionRecreoMinutos,
          DURACION_HORA_ACADEMICA_EN_MINUTOS,
          true // Es hora de salida (final del bloque)
        );

        console.log(
          `Horarios normales calculados para ${profesor.Id_Profesor_Secundaria}:`,
          {
            entrada: horaEntrada.toISOString(),
            salida: horaSalida.toISOString(),
          }
        );

        return {
          Id_Profesor_Secundaria: profesor.Id_Profesor_Secundaria,
          Nombres: profesor.Nombres,
          Apellidos: profesor.Apellidos,
          Genero: profesor.Genero,
          Google_Drive_Foto_ID: profesor.Google_Drive_Foto_ID,
          Hora_Entrada_Dia_Actual: horaEntrada,
          Hora_Salida_Dia_Actual: horaSalida,
          Aula: profesor.Id_Aula
            ? {
                Id_Aula: profesor.Id_Aula,
                Color: profesor.Color,
                Grado: profesor.Grado,
                Nivel: profesor.Nivel,
                Seccion: profesor.Seccion,
              }
            : null,
        };
      } catch (error) {
        console.error(
          `Error procesando horarios para ${profesor.Id_Profesor_Secundaria}:`,
          error
        );

        // Crear horarios predeterminados para caso de error
        const horaInicioDefault = new Date(fecha);
        horaInicioDefault.setHours(13, 0, 0, 0); // 1:00 PM

        const horaFinDefault = new Date(fecha);
        horaFinDefault.setHours(18, 30, 0, 0); // 6:30 PM

        return {
          Id_Profesor_Secundaria: profesor.Id_Profesor_Secundaria,
          Nombres: profesor.Nombres,
          Apellidos: profesor.Apellidos,
          Genero: profesor.Genero,
          Google_Drive_Foto_ID: profesor.Google_Drive_Foto_ID,
          Hora_Entrada_Dia_Actual: horaInicioDefault,
          Hora_Salida_Dia_Actual: horaFinDefault,
          Aula: profesor.Id_Aula
            ? {
                Id_Aula: profesor.Id_Aula,
                Color: profesor.Color,
                Grado: profesor.Grado,
                Nivel: profesor.Nivel,
                Seccion: profesor.Seccion,
              }
            : null,
          _error:
            "Error calculando horarios: " +
            (error instanceof Error ? error.message : String(error)),
        };
      }
    });
  } catch (error) {
    console.error(
      "Error general en obtenerProfesoresSecundariaParaTomarAsistencia:",
      error
    );
    // Siempre devolvemos array vacío en caso de error
    return [];
  }
}
