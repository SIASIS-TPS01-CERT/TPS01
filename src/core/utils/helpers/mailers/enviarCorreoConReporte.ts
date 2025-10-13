import nodemailer from "nodemailer";
import { ConfiguracionesReportes } from "../../../databases/queries/RDP02/ajustes-generales/obtenerConfiguracionesReportesEscolares";
import {
  ASUNTO_REPORTE_FALTAS,
  ASUNTO_REPORTE_TARDANZAS,
  DELAY_REINTENTO_MS,
  MAX_REINTENTOS_ENVIO_CORREO,
  NOMBRE_REMITENTE,
} from "../../../../constants/CONFIGURACION_REPORTES_ASISTENCIA_ESCOLAR";

export async function enviarCorreoConReporte(
  destinatarios: string[],
  nombreDestinatario: string,
  rolDestinatario: string,
  archivoBuffer: Buffer,
  tipoReporte: "faltas" | "tardanzas",
  nivelEducativo: string,
  configuraciones: ConfiguracionesReportes,
  aulaEspecifica?: string
): Promise<boolean> {
  let intentos = 0;

  while (intentos < MAX_REINTENTOS_ENVIO_CORREO) {
    try {
      if (
        !process.env.SE01_SIASIS_EMAIL_USER ||
        !process.env.SE01_SIASIS_EMAIL_APPLICATION_PASSWORD
      ) {
        throw new Error("Credenciales de correo no configuradas");
      }

      const transporter = nodemailer.createTransport({
        host: "smtp.gmail.com",
        port: 587,
        secure: false,
        auth: {
          user: process.env.SE01_SIASIS_EMAIL_USER,
          pass: process.env.SE01_SIASIS_EMAIL_APPLICATION_PASSWORD,
        },
      });

      const asunto =
        tipoReporte === "faltas"
          ? ASUNTO_REPORTE_FALTAS
          : ASUNTO_REPORTE_TARDANZAS;

      const nombreArchivo =
        tipoReporte === "faltas"
          ? `Reporte_Faltas_${nivelEducativo}_${
              new Date().toISOString().split("T")[0]
            }.xlsx`
          : `Reporte_Tardanzas_${nivelEducativo}_${
              new Date().toISOString().split("T")[0]
            }.xlsx`;

      const umbral =
        tipoReporte === "faltas"
          ? configuraciones.faltasConsecutivasMaximas
          : configuraciones.tardanzasConsecutivasMaximas;

      const colorTitulo = tipoReporte === "faltas" ? "DC2626" : "EA580C";
      const colorFondo = tipoReporte === "faltas" ? "FEE2E2" : "FFEDD5";
      const emoji = tipoReporte === "faltas" ? "❌" : "⏰";

      const htmlContent = `
        <!DOCTYPE html>
        <html>
        <head>
          <meta charset="UTF-8">
          <style>
            body {
              font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
              line-height: 1.6;
              color: #333;
              margin: 0;
              padding: 0;
              background-color: #f9f9f9;
            }
            .container {
              max-width: 600px;
              margin: 20px auto;
              background-color: #ffffff;
              border-radius: 12px;
              box-shadow: 0 4px 20px rgba(0, 0, 0, 0.08);
              overflow: hidden;
            }
            .header {
              background-color: #${colorTitulo};
              color: white;
              padding: 30px;
              text-align: center;
            }
            .header h1 {
              margin: 0 0 10px 0;
              font-size: 24px;
            }
            .header p {
              margin: 0;
              font-size: 16px;
              opacity: 0.9;
            }
            .content {
              padding: 30px;
            }
            .greeting {
              font-size: 16px;
              margin-bottom: 20px;
            }
            .alert-box {
              background-color: #${colorFondo};
              border-left: 4px solid #${colorTitulo};
              padding: 15px;
              margin: 20px 0;
              border-radius: 4px;
            }
            .alert-box strong {
              color: #${colorTitulo};
              font-size: 18px;
            }
            .info-box {
              background-color: #f0f9ff;
              border: 1px solid #bae6fd;
              padding: 15px;
              margin: 20px 0;
              border-radius: 4px;
            }
            .info-box p {
              margin: 5px 0;
            }
            .footer {
              background-color: #f9fafb;
              padding: 20px 30px;
              text-align: center;
              font-size: 12px;
              color: #6b7280;
              border-top: 1px solid #e5e7eb;
            }
          </style>
        </head>
        <body>
          <div class="container">
            <div class="header">
              <h1>${emoji} Reporte de ${
        tipoReporte === "faltas" ? "Faltas" : "Tardanzas"
      } Consecutivas</h1>
              <p>I.E. 20935 Asunción 8 - ${nivelEducativo}</p>
            </div>
            
            <div class="content">
              <div class="greeting">
                <p>Estimado(a) <strong>${nombreDestinatario}</strong>,</p>
                <p>Reciba un cordial saludo de la Dirección de nuestra institución educativa.</p>
              </div>

              <div class="alert-box">
                <strong>${emoji} ALERTA IMPORTANTE</strong>
                <p>Se han detectado estudiantes ${
                  aulaEspecifica
                    ? `del aula ${aulaEspecifica}`
                    : "de " + nivelEducativo
                } que han acumulado <strong>${umbral} ${
        tipoReporte === "faltas" ? "faltas" : "tardanzas"
      } consecutivas</strong>, alcanzando el umbral de alerta configurado en el sistema.</p>
              </div>

              ${
                tipoReporte === "tardanzas"
                  ? `
              <div class="info-box">
                <p><strong>ℹ️ Información importante sobre tardanzas:</strong></p>
                <p>• Tolerancia aplicada: <strong>${configuraciones.toleranciaTardanzaMinutos} minutos</strong></p>
                <p>• Hora de inicio de clases: <strong>${configuraciones.horaInicioClases}</strong></p>
                <p>• Solo se consideran tardanzas los retrasos superiores a la tolerancia establecida</p>
              </div>
              `
                  : ""
              }

              <p>Adjunto a este correo encontrará un reporte detallado en formato Excel con la siguiente información:</p>
              <ul>
                <li>Estudiantes afectados organizados por grado y sección</li>
                <li>Fechas específicas de las ${
                  tipoReporte === "faltas"
                    ? "inasistencias"
                    : "llegadas tardías"
                }</li>
                ${
                  tipoReporte === "tardanzas"
                    ? "<li>Horas de llegada registradas</li>"
                    : ""
                }
                <li>Resumen estadístico por aula</li>
              </ul>

              <p>Le solicitamos revisar esta información y, de ser necesario, tomar las acciones correspondientes según los protocolos establecidos por la institución.</p>

              <p>Para cualquier consulta o aclaración, no dude en contactar con la Dirección.</p>

              <div style="margin-top: 30px; padding-top: 20px; border-top: 1px solid #e5e7eb;">
                <p><strong>Atentamente,</strong></p>
                <p><strong>Dirección</strong><br>
                I.E. 20935 Asunción 8<br>
                Imperial, Cañete</p>
              </div>
            </div>

            <div class="footer">
              <p>Este es un correo automático generado por el Sistema de Control de Asistencia.</p>
              <p>Fecha de generación: ${new Date().toLocaleString("es-PE")}</p>
              <p>© ${new Date().getFullYear()} I.E. 20935 Asunción 8. Todos los derechos reservados.</p>
            </div>
          </div>
        </body>
        </html>
      `;

      const mailOptions = {
        from: `"${NOMBRE_REMITENTE}" <${process.env.SE01_SIASIS_EMAIL_USER}>`,
        to: destinatarios.join(","),
        subject: asunto,
        html: htmlContent,
        attachments: [
          {
            filename: nombreArchivo,
            content: archivoBuffer,
            contentType:
              "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
          },
        ],
      };

      await transporter.sendMail(mailOptions);
      return true;
    } catch (error) {
      intentos++;
      console.error(
        `      ⚠️  Intento ${intentos}/${MAX_REINTENTOS_ENVIO_CORREO} fallido:`,
        error
      );

      if (intentos < MAX_REINTENTOS_ENVIO_CORREO) {
        await new Promise((resolve) => setTimeout(resolve, DELAY_REINTENTO_MS));
      }
    }
  }

  return false;
}
