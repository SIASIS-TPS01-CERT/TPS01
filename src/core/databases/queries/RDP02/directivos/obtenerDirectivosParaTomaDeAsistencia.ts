import { T_Vacaciones_Interescolares } from "@prisma/client";
import { DirectivoParaTomaDeAsistencia } from "../../../../../interfaces/shared/Asistencia/DatosAsistenciaHoyIE20935";
import { extraerHora } from "../../../../utils/dates/modificacionFechas";
import RDP02_DB_INSTANCES from "../../../connectors/postgres";
import { verificarDentroVacacionesInterescolares } from "../../../../utils/helpers/verificators/verificarDentroVacacionesInterescolares";
import { verificarDentroSemanaGestion } from "../../../../utils/helpers/verificators/verificarDentroSemanaGestion";

export async function obtenerDirectivosParaTomarAsistencia(
  fechaActual: Date,
  vacacionesInterescolares: T_Vacaciones_Interescolares[],
  semanaGestion: any | null
): Promise<DirectivoParaTomaDeAsistencia[]> {
  try {
    console.log("==========================================");
    console.log("ANÁLISIS DE FECHAS EN obtenerDirectivosParaTomarAsistencia");
    console.log(`Fecha recibida: ${fechaActual.toISOString()}`);
    console.log(`  -> Día UTC: ${fechaActual.getUTCDate()}`);
    console.log(`  -> Mes UTC: ${fechaActual.getUTCMonth() + 1}`);
    console.log(`  -> Año UTC: ${fechaActual.getUTCFullYear()}`);
    console.log(
      `  -> Hora UTC: ${fechaActual.getUTCHours()}:${fechaActual.getUTCMinutes()}`
    );

    // 📅 CALCULAR DÍA DE LA SEMANA (1=Lunes, 2=Martes, 3=Miércoles, 4=Jueves, 5=Viernes)
    const diaSemanaJS = fechaActual.getUTCDay(); // 0=Domingo, 1=Lunes, ..., 6=Sábado
    const diaSemanaDB = diaSemanaJS === 0 ? 7 : diaSemanaJS; // Convertir: 1-7 donde 1=Lunes, 7=Domingo

    console.log(
      `  -> Día de la semana JS: ${diaSemanaJS} (0=Dom, 1=Lun, ..., 6=Sáb)`
    );
    console.log(
      `  -> Día de la semana DB: ${diaSemanaDB} (1=Lun, 2=Mar, ..., 7=Dom)`
    );

    // Solo procesar si es día laboral (Lunes a Viernes = 1 a 5)
    if (diaSemanaDB < 1 || diaSemanaDB > 5) {
      console.log(
        "⚠️  No es día laboral (solo Lunes-Viernes), devolviendo array vacío"
      );
      return [];
    }

    console.log("==========================================");

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
    let horaInicioEspecial: string | null = null;
    let horaFinEspecial: string | null = null;

    if (enVacacionesInterescolares || enSemanaGestion) {
      // Consulta para obtener los horarios especiales
      const periodoTipo = enVacacionesInterescolares
        ? "Vacaciones_Interescolares"
        : "Semana_Gestion";

      const sql = `
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

      const result = await RDP02_DB_INSTANCES.query(sql);
      console.log(
        "Resultados horarios especiales para directivos:",
        result.rows
      );

      // Extraer los valores
      for (const row of result.rows) {
        if (row.Nombre.includes("Inicio")) {
          horaInicioEspecial = extraerHora(row.Valor);
          console.log(`Hora inicio especial extraída: ${horaInicioEspecial}`);
        } else if (row.Nombre.includes("Fin")) {
          horaFinEspecial = extraerHora(row.Valor);
          console.log(`Hora fin especial extraída: ${horaFinEspecial}`);
        }
      }
    }

    // 🔄 CONSULTA: JOIN con tabla de horarios por días para directivos
    const sql = `
      SELECT 
        d."Id_Directivo",
        d."Identificador_Nacional", 
        d."Genero", 
        d."Nombres", 
        d."Apellidos", 
        d."Google_Drive_Foto_ID",
        h."Hora_Inicio" as "Horario_Laboral_Entrada",
        h."Hora_Fin" as "Horario_Laboral_Salida"
      FROM "T_Directivos" d
      LEFT JOIN "T_Horarios_Por_Dias_Directivos" h 
        ON d."Id_Directivo" = h."Id_Directivo" 
        AND h."Dia" = $1
      WHERE h."Dia" IS NOT NULL  -- Solo incluir directivos que tengan horario para este día
    `;

    const result = await RDP02_DB_INSTANCES.query(sql, [diaSemanaDB]);

    console.log(
      `📊 Directivos encontrados para día ${diaSemanaDB}:`,
      result.rows.length
    );
    console.log(
      "Datos brutos de horarios laborales de directivos:",
      result.rows.map(
        (r: {
          Id_Directivo: number;
          Identificador_Nacional: string;
          Horario_Laboral_Entrada: any;
          Horario_Laboral_Salida: any;
        }) => ({
          id: r.Id_Directivo,
          identificadorNacional: r.Identificador_Nacional,
          entrada: r.Horario_Laboral_Entrada,
          salida: r.Horario_Laboral_Salida,
        })
      )
    );

    // Procesar los resultados para combinar fecha y hora
    return result.rows.map((row: any) => {
      try {
        // Extraer solo la fecha (YYYY-MM-DD) de fechaActual
        const fecha = fechaActual.toISOString().split("T")[0];

        let horaEntrada: string;
        let horaSalida: string;

        // Si estamos en un periodo especial, usamos los horarios especiales
        if (
          (enVacacionesInterescolares || enSemanaGestion) &&
          horaInicioEspecial &&
          horaFinEspecial
        ) {
          horaEntrada = horaInicioEspecial;
          horaSalida = horaFinEspecial;
          console.log(
            `Usando horarios especiales para directivo ${row.Id_Directivo}: ${horaEntrada}, ${horaSalida}`
          );
        } else {
          // Extraer horas normales desde la nueva tabla
          horaEntrada = extraerHora(row.Horario_Laboral_Entrada) || "08:00:00";
          horaSalida = extraerHora(row.Horario_Laboral_Salida) || "16:00:00";
          console.log(
            `Horas normales extraídas para directivo ${row.Id_Directivo}: ${horaEntrada}, ${horaSalida}`
          );
        }

        // Usar valores predeterminados si no se pudo extraer
        if (!horaEntrada) horaEntrada = "08:00:00";
        if (!horaSalida) horaSalida = "16:00:00";

        // Crear fechas ISO combinando la fecha con las horas
        const horaEntradaISO = new Date(`${fecha}T${horaEntrada}.000Z`);
        const horaSalidaISO = new Date(`${fecha}T${horaSalida}.000Z`);

        console.log(`Fechas ISO para directivo ${row.Id_Directivo}:`, {
          entrada: horaEntradaISO.toISOString(),
          salida: horaSalidaISO.toISOString(),
        });

        return {
          Id_Directivo: row.Id_Directivo,
          Identificador_Nacional: row.Identificador_Nacional,
          Genero: row.Genero,
          Nombres: row.Nombres,
          Apellidos: row.Apellidos,
          Google_Drive_Foto_ID: row.Google_Drive_Foto_ID,
          Hora_Entrada_Dia_Actual: horaEntradaISO,
          Hora_Salida_Dia_Actual: horaSalidaISO,
        };
      } catch (error) {
        console.error(`Error procesando directivo ${row.Id_Directivo}:`, error);

        // Crear fechas ISO predeterminadas con el día correcto
        const fecha = fechaActual.toISOString().split("T")[0];
        const entradaPredeterminada = new Date(`${fecha}T08:00:00.000Z`);
        const salidaPredeterminada = new Date(`${fecha}T16:00:00.000Z`);

        return {
          Id_Directivo: row.Id_Directivo,
          Identificador_Nacional: row.Identificador_Nacional,
          Genero: row.Genero,
          Nombres: row.Nombres,
          Apellidos: row.Apellidos,
          Google_Drive_Foto_ID: row.Google_Drive_Foto_ID,
          Hora_Entrada_Dia_Actual: entradaPredeterminada,
          Hora_Salida_Dia_Actual: salidaPredeterminada,
          _error:
            "Error en procesamiento: " +
            (error instanceof Error ? error.message : String(error)),
        };
      }
    });
  } catch (error) {
    console.error(
      "Error general en obtenerDirectivosParaTomarAsistencia:",
      error
    );
    return []; // Devolver array vacío en caso de error general
  }
}
