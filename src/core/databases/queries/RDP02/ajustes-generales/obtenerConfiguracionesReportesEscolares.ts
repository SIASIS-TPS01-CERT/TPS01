import { NivelEducativo } from "../../../../../interfaces/shared/NivelEducativo";
import RDP02_DB_INSTANCES from "../../../connectors/postgres";

export interface ConfiguracionesReportes {
  // Configuraciones de envío de correos
  enviarCorreoFaltasDirectivos: boolean;
  enviarCorreoTardanzasDirectivos: boolean;
  enviarCorreoFaltasTutores: boolean;
  enviarCorreoTardanzasTutores: boolean;
  enviarCorreoFaltasAuxiliares: boolean;
  enviarCorreoTardanzasAuxiliares: boolean;
  enviarCorreoFaltasProfesores: boolean;
  enviarCorreoTardanzasProfesores: boolean;

  // Umbrales de alertas
  faltasConsecutivasMaximas: number;
  tardanzasConsecutivasMaximas: number;

  // Configuraciones de horario
  toleranciaTardanzaMinutos: number;
  horaInicioClases: string;
}

export async function obtenerConfiguracionesReportesEscolares(
  nivel: NivelEducativo
): Promise<ConfiguracionesReportes> {
  try {
    const nivelTexto =
      nivel === NivelEducativo.SECUNDARIA ? "SECUNDARIA" : "PRIMARIA";

    // Nombres de configuraciones a obtener
    const nombresConfig = [
      "ENVIAR_CORREO_REPORTE_ESCOLAR_FALTAS_CONSECUTIVAS_DIRECTIVOS",
      "ENVIAR_CORREO_REPORTE_ESCOLAR_TARDANZAS_CONSECUTIVAS_DIRECTIVOS",
      `ENVIAR_CORREO_REPORTE_ESCOLAR_FALTAS_CONSECUTIVAS_${
        nivel === NivelEducativo.SECUNDARIA
          ? "TUTORES_SECUNDARIA"
          : "PROFESOR_PRIMARIA"
      }`,
      `ENVIAR_CORREO_REPORTE_ESCOLAR_TARDANZAS_CONSECUTIVAS_${
        nivel === NivelEducativo.SECUNDARIA
          ? "TUTORES_SECUNDARIA"
          : "PROFESOR_PRIMARIA"
      }`,
      `FALTAS_CONSECUTIVAS_MAXIMAS_ALERTA_ESTUDIANTES_${nivelTexto}`,
      `TARDANZAS_CONSECUTIVAS_MAXIMAS_ALERTA_ESTUDIANTES_${nivelTexto}`,
      `TOLERANCIA_TARDANZA_MINUTOS_${nivelTexto}`,
    ];

    // Agregar auxiliares solo para secundaria
    if (nivel === NivelEducativo.SECUNDARIA) {
      nombresConfig.push(
        "ENVIAR_CORREO_REPORTE_ESCOLAR_FALTAS_CONSECUTIVAS_AUXILIARES",
        "ENVIAR_CORREO_REPORTE_ESCOLAR_TARDANZAS_CONSECUTIVAS_AUXILIARES"
      );
    }

    const sql = `
      SELECT "Nombre", "Valor"
      FROM "T_Ajustes_Generales_Sistema"
      WHERE "Nombre" = ANY($1)
    `;

    const result = await RDP02_DB_INSTANCES.query(sql, [nombresConfig]);

    // Convertir a objeto para fácil acceso
    const configs: any = {};
    result.rows.forEach((row: any) => {
      configs[row.Nombre] = row.Valor;
    });

    // Obtener hora de inicio de clases
    const sqlHorario = `
      SELECT "Valor"
      FROM "T_Horarios_Asistencia"
      WHERE "Nombre" = $1
    `;

    const nombreHorario =
      nivel === NivelEducativo.SECUNDARIA
        ? "Hora_Inicio_Asistencia_Secundaria"
        : "Hora_Inicio_Asistencia_Primaria";

    const resultHorario = await RDP02_DB_INSTANCES.query(sqlHorario, [
      nombreHorario,
    ]);
    const horaInicio = resultHorario.rows[0]?.Valor || "08:00:00";

    return {
      enviarCorreoFaltasDirectivos:
        configs[
          "ENVIAR_CORREO_REPORTE_ESCOLAR_FALTAS_CONSECUTIVAS_DIRECTIVOS"
        ] === "ACTIVADO",
      enviarCorreoTardanzasDirectivos:
        configs[
          "ENVIAR_CORREO_REPORTE_ESCOLAR_TARDANZAS_CONSECUTIVAS_DIRECTIVOS"
        ] === "ACTIVADO",

      enviarCorreoFaltasTutores:
        nivel === NivelEducativo.SECUNDARIA
          ? configs[
              "ENVIAR_CORREO_REPORTE_ESCOLAR_FALTAS_CONSECUTIVAS_TUTORES_SECUNDARIA"
            ] === "ACTIVADO"
          : false,
      enviarCorreoTardanzasTutores:
        nivel === NivelEducativo.SECUNDARIA
          ? configs[
              "ENVIAR_CORREO_REPORTE_ESCOLAR_TARDANZAS_CONSECUTIVAS_TUTORES_SECUNDARIA"
            ] === "ACTIVADO"
          : false,

      enviarCorreoFaltasAuxiliares:
        nivel === NivelEducativo.SECUNDARIA
          ? configs[
              "ENVIAR_CORREO_REPORTE_ESCOLAR_FALTAS_CONSECUTIVAS_AUXILIARES"
            ] === "ACTIVADO"
          : false,
      enviarCorreoTardanzasAuxiliares:
        nivel === NivelEducativo.SECUNDARIA
          ? configs[
              "ENVIAR_CORREO_REPORTE_ESCOLAR_TARDANZAS_CONSECUTIVAS_AUXILIARES"
            ] === "ACTIVADO"
          : false,

      enviarCorreoFaltasProfesores:
        nivel === NivelEducativo.PRIMARIA
          ? configs[
              "ENVIAR_CORREO_REPORTE_ESCOLAR_FALTAS_CONSECUTIVAS_PROFESOR_PRIMARIA"
            ] === "ACTIVADO"
          : false,
      enviarCorreoTardanzasProfesores:
        nivel === NivelEducativo.PRIMARIA
          ? configs[
              "ENVIAR_CORREO_REPORTE_ESCOLAR_TARDANZAS_CONSECUTIVAS_PROFESOR_PRIMARIA"
            ] === "ACTIVADO"
          : false,

      faltasConsecutivasMaximas: parseInt(
        configs[
          `FALTAS_CONSECUTIVAS_MAXIMAS_ALERTA_ESTUDIANTES_${nivelTexto}`
        ] || "3"
      ),
      tardanzasConsecutivasMaximas: parseInt(
        configs[
          `TARDANZAS_CONSECUTIVAS_MAXIMAS_ALERTA_ESTUDIANTES_${nivelTexto}`
        ] || "3"
      ),
      toleranciaTardanzaMinutos: parseInt(
        configs[`TOLERANCIA_TARDANZA_MINUTOS_${nivelTexto}`] || "5"
      ),
      horaInicioClases: horaInicio,
    };
  } catch (error) {
    console.error("❌ Error obteniendo configuraciones de reportes:", error);
    throw error;
  }
}
