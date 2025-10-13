import { DELAY_ENTRE_CORREOS_MS, ENVIAR_CORREOS_EN_GRUPO, ENVIAR_EXCEL_COMPLETO_A_TUTORES_Y_PROFESORES } from "../../../../constants/CONFIGURACION_REPORTES_ASISTENCIA_ESCOLAR";
import { NivelEducativo } from "../../../../interfaces/shared/NivelEducativo";
import { ConfiguracionesReportes } from "../../../databases/queries/RDP02/ajustes-generales/obtenerConfiguracionesReportesEscolares";
import { DestinatariosCorreos } from "../../../databases/queries/RDP02/personal-en-general/obtenerCorreosPersonalAutorizadoAlertasAsistenciasEscolares";
import { ReportesExcel } from "../generators/generarReportesAlertaAsistenciaEscolarEnExcel";
import { enviarCorreoConReporte } from "./enviarCorreoConReporte";

export interface ResultadoEnvio {
  exitosos: number;
  fallidos: number;
  omitidos: number;
}

export async function enviarCorreosReportes(
  nivel: NivelEducativo,
  configuraciones: ConfiguracionesReportes,
  reportes: ReportesExcel,
  destinatarios: DestinatariosCorreos,
  enviarFaltas: boolean,
  enviarTardanzas: boolean
): Promise<ResultadoEnvio> {
  try {
    const resultado: ResultadoEnvio = {
      exitosos: 0,
      fallidos: 0,
      omitidos: 0,
    };

    const nivelTexto =
      nivel === NivelEducativo.SECUNDARIA ? "SECUNDARIA" : "PRIMARIA";

    // ===== ENVIAR A DIRECTIVOS =====
    if (
      (enviarFaltas && configuraciones.enviarCorreoFaltasDirectivos) ||
      (enviarTardanzas && configuraciones.enviarCorreoTardanzasDirectivos)
    ) {
      console.log("\n   📧 Enviando correos a directivos...");

      if (ENVIAR_CORREOS_EN_GRUPO && destinatarios.directivos.length > 0) {
        // Envío grupal a todos los directivos
        const correos = destinatarios.directivos.map((d) => d.correo);
        const nombres = destinatarios.directivos
          .map((d) => `${d.nombres} ${d.apellidos}`)
          .join(", ");

        const exitoFaltas = enviarFaltas
          ? await enviarCorreoConReporte(
              correos,
              nombres,
              "Directivos",
              reportes.reporteFaltas!.completo,
              "faltas",
              nivelTexto,
              configuraciones
            )
          : true;

        const exitoTardanzas = enviarTardanzas
          ? await enviarCorreoConReporte(
              correos,
              nombres,
              "Directivos",
              reportes.reporteTardanzas!.completo,
              "tardanzas",
              nivelTexto,
              configuraciones
            )
          : true;

        if (exitoFaltas && exitoTardanzas) {
          resultado.exitosos++;
          console.log(
            `      ✅ Enviado a ${destinatarios.directivos.length} directivos (grupal)`
          );
        } else {
          resultado.fallidos++;
        }
      } else {
        // Envío individual a cada directivo
        for (const directivo of destinatarios.directivos) {
          const nombreCompleto = `${directivo.nombres} ${directivo.apellidos}`;

          let exitoso = true;

          if (enviarFaltas && reportes.reporteFaltas) {
            exitoso =
              exitoso &&
              (await enviarCorreoConReporte(
                [directivo.correo],
                nombreCompleto,
                "Directivo",
                reportes.reporteFaltas.completo,
                "faltas",
                nivelTexto,
                configuraciones
              ));
          }

          if (enviarTardanzas && reportes.reporteTardanzas) {
            exitoso =
              exitoso &&
              (await enviarCorreoConReporte(
                [directivo.correo],
                nombreCompleto,
                "Directivo",
                reportes.reporteTardanzas.completo,
                "tardanzas",
                nivelTexto,
                configuraciones
              ));
          }

          if (exitoso) {
            resultado.exitosos++;
            console.log(`      ✅ ${nombreCompleto}`);
          } else {
            resultado.fallidos++;
            console.log(`      ❌ ${nombreCompleto}`);
          }

          await esperar(DELAY_ENTRE_CORREOS_MS);
        }
      }
    }

    // ===== ENVIAR A AUXILIARES (solo secundaria) =====
    if (nivel === NivelEducativo.SECUNDARIA) {
      if (
        (enviarFaltas && configuraciones.enviarCorreoFaltasAuxiliares) ||
        (enviarTardanzas && configuraciones.enviarCorreoTardanzasAuxiliares)
      ) {
        console.log("\n   📧 Enviando correos a auxiliares...");

        if (ENVIAR_CORREOS_EN_GRUPO && destinatarios.auxiliares.length > 0) {
          // Envío grupal
          const correos = destinatarios.auxiliares.map((d) => d.correo);
          const nombres = destinatarios.auxiliares
            .map((d) => `${d.nombres} ${d.apellidos}`)
            .join(", ");

          const exitoFaltas = enviarFaltas
            ? await enviarCorreoConReporte(
                correos,
                nombres,
                "Auxiliares",
                reportes.reporteFaltas!.completo,
                "faltas",
                nivelTexto,
                configuraciones
              )
            : true;

          const exitoTardanzas = enviarTardanzas
            ? await enviarCorreoConReporte(
                correos,
                nombres,
                "Auxiliares",
                reportes.reporteTardanzas!.completo,
                "tardanzas",
                nivelTexto,
                configuraciones
              )
            : true;

          if (exitoFaltas && exitoTardanzas) {
            resultado.exitosos++;
            console.log(
              `      ✅ Enviado a ${destinatarios.auxiliares.length} auxiliares (grupal)`
            );
          } else {
            resultado.fallidos++;
          }
        } else {
          // Envío individual
          for (const auxiliar of destinatarios.auxiliares) {
            const nombreCompleto = `${auxiliar.nombres} ${auxiliar.apellidos}`;
            let exitoso = true;

            if (enviarFaltas && reportes.reporteFaltas) {
              exitoso =
                exitoso &&
                (await enviarCorreoConReporte(
                  [auxiliar.correo],
                  nombreCompleto,
                  "Auxiliar",
                  reportes.reporteFaltas.completo,
                  "faltas",
                  nivelTexto,
                  configuraciones
                ));
            }

            if (enviarTardanzas && reportes.reporteTardanzas) {
              exitoso =
                exitoso &&
                (await enviarCorreoConReporte(
                  [auxiliar.correo],
                  nombreCompleto,
                  "Auxiliar",
                  reportes.reporteTardanzas.completo,
                  "tardanzas",
                  nivelTexto,
                  configuraciones
                ));
            }

            if (exitoso) {
              resultado.exitosos++;
              console.log(`      ✅ ${nombreCompleto}`);
            } else {
              resultado.fallidos++;
            }

            await esperar(DELAY_ENTRE_CORREOS_MS);
          }
        }
      }

      // ===== ENVIAR A TUTORES (secundaria) =====
      if (
        (enviarFaltas && configuraciones.enviarCorreoFaltasTutores) ||
        (enviarTardanzas && configuraciones.enviarCorreoTardanzasTutores)
      ) {
        console.log("\n   📧 Enviando correos a tutores de secundaria...");

        for (const tutor of destinatarios.tutores) {
          const nombreCompleto = `${tutor.nombres} ${tutor.apellidos}`;
          const aulaKey = `${tutor.grado}-${tutor.seccion}`;

          // Determinar qué reportes enviar
          const usarReporteCompleto =
            ENVIAR_EXCEL_COMPLETO_A_TUTORES_Y_PROFESORES;
          const reporteAulaData = reportes.reportesPorAula.get(aulaKey);

          if (!reporteAulaData) {
            console.log(
              `      ⚠️  No hay reporte para ${nombreCompleto} (${tutor.grado}° ${tutor.seccion})`
            );
            continue;
          }

          let exitoso = true;

          if (enviarFaltas) {
            const bufferFaltas = usarReporteCompleto
              ? reportes.reporteFaltas!.completo
              : reporteAulaData.faltas;

            if (bufferFaltas) {
              exitoso =
                exitoso &&
                (await enviarCorreoConReporte(
                  [tutor.correo],
                  nombreCompleto,
                  "Tutor",
                  bufferFaltas,
                  "faltas",
                  nivelTexto,
                  configuraciones,
                  `${tutor.grado}° "${tutor.seccion}"`
                ));
            }
          }

          if (enviarTardanzas) {
            const bufferTardanzas = usarReporteCompleto
              ? reportes.reporteTardanzas!.completo
              : reporteAulaData.tardanzas;

            if (bufferTardanzas) {
              exitoso =
                exitoso &&
                (await enviarCorreoConReporte(
                  [tutor.correo],
                  nombreCompleto,
                  "Tutor",
                  bufferTardanzas,
                  "tardanzas",
                  nivelTexto,
                  configuraciones,
                  `${tutor.grado}° "${tutor.seccion}"`
                ));
            }
          }

          if (exitoso) {
            resultado.exitosos++;
            console.log(
              `      ✅ ${nombreCompleto} (${tutor.grado}° ${tutor.seccion})`
            );
          } else {
            resultado.fallidos++;
          }

          await esperar(DELAY_ENTRE_CORREOS_MS);
        }
      }
    } else {
      // ===== ENVIAR A PROFESORES (primaria) =====
      if (
        (enviarFaltas && configuraciones.enviarCorreoFaltasProfesores) ||
        (enviarTardanzas && configuraciones.enviarCorreoTardanzasProfesores)
      ) {
        console.log("\n   📧 Enviando correos a profesores de primaria...");

        for (const profesor of destinatarios.profesoresPrimaria) {
          const nombreCompleto = `${profesor.nombres} ${profesor.apellidos}`;
          const aulaKey = `${profesor.grado}-${profesor.seccion}`;

          const usarReporteCompleto =
            ENVIAR_EXCEL_COMPLETO_A_TUTORES_Y_PROFESORES;
          const reporteAulaData = reportes.reportesPorAula.get(aulaKey);

          if (!reporteAulaData) {
            console.log(
              `      ⚠️  No hay reporte para ${nombreCompleto} (${profesor.grado}° ${profesor.seccion})`
            );
            continue;
          }

          let exitoso = true;

          if (enviarFaltas) {
            const bufferFaltas = usarReporteCompleto
              ? reportes.reporteFaltas!.completo
              : reporteAulaData.faltas;

            if (bufferFaltas) {
              exitoso =
                exitoso &&
                (await enviarCorreoConReporte(
                  [profesor.correo],
                  nombreCompleto,
                  "Profesor",
                  bufferFaltas,
                  "faltas",
                  nivelTexto,
                  configuraciones,
                  `${profesor.grado}° "${profesor.seccion}"`
                ));
            }
          }

          if (enviarTardanzas) {
            const bufferTardanzas = usarReporteCompleto
              ? reportes.reporteTardanzas!.completo
              : reporteAulaData.tardanzas;

            if (bufferTardanzas) {
              exitoso =
                exitoso &&
                (await enviarCorreoConReporte(
                  [profesor.correo],
                  nombreCompleto,
                  "Profesor",
                  bufferTardanzas,
                  "tardanzas",
                  nivelTexto,
                  configuraciones,
                  `${profesor.grado}° "${profesor.seccion}"`
                ));
            }
          }

          if (exitoso) {
            resultado.exitosos++;
            console.log(
              `      ✅ ${nombreCompleto} (${profesor.grado}° ${profesor.seccion})`
            );
          } else {
            resultado.fallidos++;
          }

          await esperar(DELAY_ENTRE_CORREOS_MS);
        }
      }
    }

    return resultado;
  } catch (error) {
    console.error("❌ Error enviando correos:", error);
    throw error;
  }
}

function esperar(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
