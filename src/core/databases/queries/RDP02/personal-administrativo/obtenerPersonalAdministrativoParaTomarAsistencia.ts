import { T_Vacaciones_Interescolares } from "@prisma/client";
import { PersonalAdministrativoParaTomaDeAsistencia } from "../../../../../interfaces/shared/Asistencia/DatosAsistenciaHoyIE20935";
import { extraerHora } from "../../../../utils/dates/modificacionFechas";
import { verificarDentroSemanaGestion } from "../../../../utils/verificators/verificarDentroSemanaGestion";
import { verificarDentroVacacionesInterescolares } from "../../../../utils/verificators/verificarDentroVacacionesInterescolares";
import RDP02_DB_INSTANCES from "../../../connectors/postgres";

export async function obtenerPersonalAdministrativoParaTomarAsistencia(
  fechaActual: Date,
  vacacionesInterescolares: T_Vacaciones_Interescolares[],
  semanaGestion: any | null
): Promise<PersonalAdministrativoParaTomaDeAsistencia[]> {
  try {
    console.log("==========================================");
    console.log(
      "ANÁLISIS DE FECHAS EN obtenerPersonalAdministrativoParaTomarAsistencia"
    );
    console.log(`Fecha recibida: ${fechaActual.toISOString()}`);
    console.log(`  -> Día UTC: ${fechaActual.getUTCDate()}`);
    console.log(`  -> Mes UTC: ${fechaActual.getUTCMonth() + 1}`);
    console.log(`  -> Año UTC: ${fechaActual.getUTCFullYear()}`);
    console.log(
      `  -> Hora UTC: ${fechaActual.getUTCHours()}:${fechaActual.getUTCMinutes()}`
    );
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
      console.log("Resultados horarios especiales:", result.rows);

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
    console.log(
      "Datos brutos de horarios laborales:",
      result.rows.map(
        (r: {
          DNI_Personal_Administrativo: any;
          Horario_Laboral_Entrada: any;
          Horario_Laboral_Salida: any;
        }) => ({
          dni: r.DNI_Personal_Administrativo,
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
            `Usando horarios especiales para ${row.DNI_Personal_Administrativo}: ${horaEntrada}, ${horaSalida}`
          );
        } else {
          // Extraer horas normales
          horaEntrada = extraerHora(row.Horario_Laboral_Entrada) || "08:00:00";
          horaSalida = extraerHora(row.Horario_Laboral_Salida) || "16:00:00";
          console.log(
            `Horas normales extraídas para ${row.DNI_Personal_Administrativo}: ${horaEntrada}, ${horaSalida}`
          );
        }

        // Usar valores predeterminados si no se pudo extraer
        if (!horaEntrada) horaEntrada = "08:00:00";
        if (!horaSalida) horaSalida = "16:00:00";

        // Crear fechas ISO combinando la fecha con las horas
        const horaEntradaISO = new Date(`${fecha}T${horaEntrada}.000Z`);
        const horaSalidaISO = new Date(`${fecha}T${horaSalida}.000Z`);

        console.log(`Fechas ISO para ${row.DNI_Personal_Administrativo}:`, {
          entrada: horaEntradaISO.toISOString(),
          salida: horaSalidaISO.toISOString(),
        });

        return {
          DNI_Personal_Administrativo: row.DNI_Personal_Administrativo,
          Genero: row.Genero,
          Nombres: row.Nombres,
          Apellidos: row.Apellidos,
          Cargo: row.Cargo,
          Google_Drive_Foto_ID: row.Google_Drive_Foto_ID,
          Hora_Entrada_Dia_Actual: horaEntradaISO.toISOString(),
          Hora_Salida_Dia_Actual: horaSalidaISO.toISOString(),
        };
      } catch (error) {
        console.error(
          `Error procesando ${row.DNI_Personal_Administrativo}:`,
          error
        );

        // Crear fechas ISO predeterminadas con el día correcto
        const fecha = fechaActual.toISOString().split("T")[0];
        const entradaPredeterminada = new Date(`${fecha}T08:00:00.000Z`);
        const salidaPredeterminada = new Date(`${fecha}T16:00:00.000Z`);

        return {
          DNI_Personal_Administrativo: row.DNI_Personal_Administrativo,
          Genero: row.Genero,
          Nombres: row.Nombres,
          Apellidos: row.Apellidos,
          Cargo: row.Cargo,
          Google_Drive_Foto_ID: row.Google_Drive_Foto_ID,
          Hora_Entrada_Dia_Actual: entradaPredeterminada.toISOString(),
          Hora_Salida_Dia_Actual: salidaPredeterminada.toISOString(),
          _error:
            "Error en procesamiento: " +
            (error instanceof Error ? error.message : String(error)),
        };
      }
    });
  } catch (error) {
    console.error(
      "Error general en obtenerPersonalAdministrativoParaTomarAsistencia:",
      error
    );
    return []; // Devolver array vacío en caso de error general
  }
}
