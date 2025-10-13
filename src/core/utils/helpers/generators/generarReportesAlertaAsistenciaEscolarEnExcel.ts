import * as ExcelJS from "exceljs";

import { NivelEducativo } from "../../../../interfaces/shared/NivelEducativo";
import {
  EstudianteConProblema,
  ResultadosAnalisis,
} from "../analizers/analizarAsistenciasEscolaresConsecutivas";
import { ConfiguracionesReportes } from "../../../databases/queries/RDP02/ajustes-generales/obtenerConfiguracionesReportesEscolares";
import {
  NOMBRE_HOJA_UNICA,
  PREFIJO_NOMBRE_HOJA,
} from "../../../../constants/CONFIGURACION_REPORTES_ASISTENCIA_ESCOLAR";

export interface ReportesExcel {
  reporteFaltas: {
    completo: Buffer;
  } | null;
  reporteTardanzas: {
    completo: Buffer;
  } | null;
  reportesPorAula: Map<
    string,
    {
      faltas?: Buffer;
      tardanzas?: Buffer;
      grado: number;
      seccion: string;
    }
  >;
}

export async function generarReportesExcel(
  nivel: NivelEducativo,
  resultados: ResultadosAnalisis,
  configuraciones: ConfiguracionesReportes
): Promise<ReportesExcel> {
  try {
    const nivelTexto =
      nivel === NivelEducativo.SECUNDARIA ? "SECUNDARIA" : "PRIMARIA";

    const reportesPorAula = new Map<string, any>();

    // Generar reporte de faltas
    let reporteFaltas = null;
    if (resultados.estudiantesConFaltas.length > 0) {
      console.log("   📄 Generando reporte de faltas...");

      const bufferCompleto = await generarExcelFaltas(
        resultados.estudiantesConFaltas,
        configuraciones,
        nivelTexto
      );

      reporteFaltas = { completo: bufferCompleto };

      // Generar reportes individuales por aula
      const estudiantesPorAula = agruparPorAula(
        resultados.estudiantesConFaltas
      );
      for (const [aulaKey, estudiantes] of estudiantesPorAula) {
        const bufferAula = await generarExcelFaltas(
          estudiantes,
          configuraciones,
          nivelTexto,
          true
        );

        const aulaInfo = reportesPorAula.get(aulaKey) || {
          grado: estudiantes[0].grado,
          seccion: estudiantes[0].seccion,
        };
        aulaInfo.faltas = bufferAula;
        reportesPorAula.set(aulaKey, aulaInfo);
      }
    }

    // Generar reporte de tardanzas
    let reporteTardanzas = null;
    if (resultados.estudiantesConTardanzas.length > 0) {
      console.log("   📄 Generando reporte de tardanzas...");

      const bufferCompleto = await generarExcelTardanzas(
        resultados.estudiantesConTardanzas,
        configuraciones,
        nivelTexto
      );

      reporteTardanzas = { completo: bufferCompleto };

      // Generar reportes individuales por aula
      const estudiantesPorAula = agruparPorAula(
        resultados.estudiantesConTardanzas
      );
      for (const [aulaKey, estudiantes] of estudiantesPorAula) {
        const bufferAula = await generarExcelTardanzas(
          estudiantes,
          configuraciones,
          nivelTexto,
          true
        );

        const aulaInfo = reportesPorAula.get(aulaKey) || {
          grado: estudiantes[0].grado,
          seccion: estudiantes[0].seccion,
        };
        aulaInfo.tardanzas = bufferAula;
        reportesPorAula.set(aulaKey, aulaInfo);
      }
    }

    return {
      reporteFaltas,
      reporteTardanzas,
      reportesPorAula,
    };
  } catch (error) {
    console.error("❌ Error generando reportes Excel:", error);
    throw error;
  }
}

function agruparPorAula(
  estudiantes: EstudianteConProblema[]
): Map<string, EstudianteConProblema[]> {
  const mapa = new Map<string, EstudianteConProblema[]>();

  for (const estudiante of estudiantes) {
    const key = `${estudiante.grado}-${estudiante.seccion}`;
    const lista = mapa.get(key) || [];
    lista.push(estudiante);
    mapa.set(key, lista);
  }

  return mapa;
}

async function generarExcelFaltas(
  estudiantes: EstudianteConProblema[],
  config: ConfiguracionesReportes,
  nivelTexto: string,
  esReporteIndividual: boolean = false
): Promise<Buffer> {
  const workbook = new ExcelJS.Workbook();

  // Agrupar por grado si es reporte completo
  const estudiantesPorGrado = new Map<number, EstudianteConProblema[]>();

  if (esReporteIndividual) {
    // Para reporte individual, una sola hoja
    estudiantesPorGrado.set(0, estudiantes);
  } else {
    // Para reporte completo, separar por grados
    for (const estudiante of estudiantes) {
      const lista = estudiantesPorGrado.get(estudiante.grado) || [];
      lista.push(estudiante);
      estudiantesPorGrado.set(estudiante.grado, lista);
    }
  }

  // Crear una hoja por grado
  for (const [grado, estudiantesGrado] of estudiantesPorGrado) {
    const nombreHoja = esReporteIndividual
      ? NOMBRE_HOJA_UNICA
      : `${PREFIJO_NOMBRE_HOJA} ${grado}`;

    const worksheet = workbook.addWorksheet(nombreHoja);

    // ===== ENCABEZADO =====
    worksheet.mergeCells("A1:F1");
    const cellTitulo = worksheet.getCell("A1");
    cellTitulo.value = "I.E. 20935 ASUNCIÓN 8 - IMPERIAL, CAÑETE";
    cellTitulo.font = { size: 16, bold: true, color: { argb: "FFFFFF" } };
    cellTitulo.fill = {
      type: "pattern",
      pattern: "solid",
      fgColor: { argb: "DC2626" }, // Rojo para faltas
    };
    cellTitulo.alignment = { horizontal: "center", vertical: "middle" };
    cellTitulo.border = {
      top: { style: "medium" },
      left: { style: "medium" },
      bottom: { style: "medium" },
      right: { style: "medium" },
    };
    worksheet.getRow(1).height = 25;

    // Subtítulo
    worksheet.mergeCells("A2:F2");
    const cellSubtitulo = worksheet.getCell("A2");
    cellSubtitulo.value = `REPORTE DE FALTAS CONSECUTIVAS - ${nivelTexto}`;
    cellSubtitulo.font = { size: 14, bold: true, color: { argb: "FFFFFF" } };
    cellSubtitulo.fill = {
      type: "pattern",
      pattern: "solid",
      fgColor: { argb: "EF4444" },
    };
    cellSubtitulo.alignment = { horizontal: "center", vertical: "middle" };
    cellSubtitulo.border = {
      top: { style: "medium" },
      left: { style: "medium" },
      bottom: { style: "medium" },
      right: { style: "medium" },
    };
    worksheet.getRow(2).height = 20;

    // Info configuración
    worksheet.mergeCells("A3:C3");
    const cellConfig = worksheet.getCell("A3");
    cellConfig.value = `⚠️  Umbral de alerta: ${config.faltasConsecutivasMaximas} faltas consecutivas`;
    cellConfig.font = { size: 12, bold: true, color: { argb: "DC2626" } };
    cellConfig.fill = {
      type: "pattern",
      pattern: "solid",
      fgColor: { argb: "FEE2E2" },
    };
    cellConfig.alignment = { horizontal: "center", vertical: "middle" };
    cellConfig.border = {
      top: { style: "thin" },
      left: { style: "medium" },
      bottom: { style: "thin" },
      right: { style: "thin" },
    };

    worksheet.mergeCells("D3:F3");
    const cellFecha = worksheet.getCell("D3");
    cellFecha.value = `Fecha de reporte: ${new Date().toLocaleDateString(
      "es-PE"
    )}`;
    cellFecha.font = { size: 11, italic: true };
    cellFecha.alignment = { horizontal: "center", vertical: "middle" };
    cellFecha.border = {
      top: { style: "thin" },
      left: { style: "thin" },
      bottom: { style: "thin" },
      right: { style: "medium" },
    };
    worksheet.getRow(3).height = 18;

    // Separador
    worksheet.getRow(4).height = 8;

    // ===== TABLA DE DATOS =====
    // Encabezados de columnas
    const headers = [
      "Grado",
      "Sección",
      "Apellidos y Nombres",
      "Fechas con Falta",
      "Total Días",
      "Estado",
    ];
    const headerRow = worksheet.getRow(5);

    headers.forEach((header, idx) => {
      const cell = headerRow.getCell(idx + 1);
      cell.value = header;
      cell.font = { bold: true, size: 11, color: { argb: "FFFFFF" } };
      cell.fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: "991B1B" },
      };
      cell.alignment = {
        horizontal: "center",
        vertical: "middle",
        wrapText: true,
      };
      cell.border = {
        top: { style: "medium" },
        left: { style: "thin" },
        bottom: { style: "medium" },
        right: { style: "thin" },
      };
    });
    headerRow.height = 20;

    // Anchos de columnas
    worksheet.columns = [
      { width: 10 }, // Grado
      { width: 12 }, // Sección
      { width: 35 }, // Nombres
      { width: 30 }, // Fechas
      { width: 12 }, // Total
      { width: 15 }, // Estado
    ];

    // Datos de estudiantes
    let filaActual = 6;

    // Agrupar por sección
    const estudiantesPorSeccion = new Map<string, EstudianteConProblema[]>();
    for (const estudiante of estudiantesGrado) {
      const key = estudiante.seccion;
      const lista = estudiantesPorSeccion.get(key) || [];
      lista.push(estudiante);
      estudiantesPorSeccion.set(key, lista);
    }

    // Ordenar secciones
    const seccionesOrdenadas = Array.from(estudiantesPorSeccion.keys()).sort();

    for (const seccion of seccionesOrdenadas) {
      const estudiantesSeccion = estudiantesPorSeccion.get(seccion)!;

      // Cabecera de sección
      worksheet.mergeCells(`A${filaActual}:F${filaActual}`);
      const cellSeccion = worksheet.getCell(`A${filaActual}`);
      cellSeccion.value = `SECCIÓN "${seccion}"`;
      cellSeccion.font = { bold: true, size: 11, color: { argb: "FFFFFF" } };

      // Usar el color del aula
      const colorAula = estudiantesSeccion[0].colorAula;
      cellSeccion.fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: colorAula.replace("#", "") },
      };
      cellSeccion.alignment = { horizontal: "center", vertical: "middle" };
      cellSeccion.border = {
        top: { style: "medium" },
        left: { style: "medium" },
        bottom: { style: "thin" },
        right: { style: "medium" },
      };
      worksheet.getRow(filaActual).height = 18;
      filaActual++;

      // Datos de estudiantes de esta sección
      for (const estudiante of estudiantesSeccion) {
        const row = worksheet.getRow(filaActual);

        const fechasStr = estudiante.diasConsecutivos
          .map((d) => formatearFecha(d.fecha))
          .join(", ");

        row.getCell(1).value = estudiante.grado;
        row.getCell(2).value = estudiante.seccion;
        row.getCell(3).value = `${estudiante.apellidos}, ${estudiante.nombres}`;
        row.getCell(4).value = fechasStr;
        row.getCell(5).value = estudiante.diasConsecutivos.length;
        row.getCell(6).value = "❌ CRÍTICO";

        // Estilos
        [1, 2, 5].forEach((col) => {
          row.getCell(col).alignment = {
            horizontal: "center",
            vertical: "middle",
          };
        });
        row.getCell(3).alignment = {
          horizontal: "left",
          vertical: "middle",
          indent: 1,
        };
        row.getCell(4).alignment = {
          horizontal: "left",
          vertical: "middle",
          wrapText: true,
        };
        row.getCell(6).font = { bold: true, color: { argb: "DC2626" } };
        row.getCell(6).alignment = { horizontal: "center", vertical: "middle" };

        // Bordes y alternancia de color
        const bgColor = filaActual % 2 === 0 ? "FFFFFF" : "FEF2F2";
        [1, 2, 3, 4, 5, 6].forEach((col) => {
          const cell = row.getCell(col);
          cell.fill = {
            type: "pattern",
            pattern: "solid",
            fgColor: { argb: bgColor },
          };
          cell.border = {
            top: { style: "thin" },
            left: col === 1 ? { style: "medium" } : { style: "thin" },
            bottom: { style: "thin" },
            right: col === 6 ? { style: "medium" } : { style: "thin" },
          };
        });

        row.height = 25;
        filaActual++;
      }

      // Separador entre secciones
      worksheet.getRow(filaActual).height = 5;
      filaActual++;
    }

    // Resumen final
    filaActual++;
    worksheet.mergeCells(`A${filaActual}:F${filaActual}`);
    const cellResumen = worksheet.getCell(`A${filaActual}`);
    cellResumen.value = `Total de estudiantes con faltas consecutivas: ${estudiantesGrado.length}`;
    cellResumen.font = { bold: true, size: 12, color: { argb: "FFFFFF" } };
    cellResumen.fill = {
      type: "pattern",
      pattern: "solid",
      fgColor: { argb: "DC2626" },
    };
    cellResumen.alignment = { horizontal: "center", vertical: "middle" };
    cellResumen.border = {
      top: { style: "medium" },
      left: { style: "medium" },
      bottom: { style: "medium" },
      right: { style: "medium" },
    };
    worksheet.getRow(filaActual).height = 22;
  }

  const buffer = await workbook.xlsx.writeBuffer();
  return Buffer.from(buffer);
}

async function generarExcelTardanzas(
  estudiantes: EstudianteConProblema[],
  config: ConfiguracionesReportes,
  nivelTexto: string,
  esReporteIndividual: boolean = false
): Promise<Buffer> {
  const workbook = new ExcelJS.Workbook();

  // Similar estructura pero con datos de tardanzas
  const estudiantesPorGrado = new Map<number, EstudianteConProblema[]>();

  if (esReporteIndividual) {
    estudiantesPorGrado.set(0, estudiantes);
  } else {
    for (const estudiante of estudiantes) {
      const lista = estudiantesPorGrado.get(estudiante.grado) || [];
      lista.push(estudiante);
      estudiantesPorGrado.set(estudiante.grado, lista);
    }
  }

  for (const [grado, estudiantesGrado] of estudiantesPorGrado) {
    const nombreHoja = esReporteIndividual
      ? NOMBRE_HOJA_UNICA
      : `${PREFIJO_NOMBRE_HOJA} ${grado}`;

    const worksheet = workbook.addWorksheet(nombreHoja);

    // ===== ENCABEZADO =====
    worksheet.mergeCells("A1:G1");
    const cellTitulo = worksheet.getCell("A1");
    cellTitulo.value = "I.E. 20935 ASUNCIÓN 8 - IMPERIAL, CAÑETE";
    cellTitulo.font = { size: 16, bold: true, color: { argb: "FFFFFF" } };
    cellTitulo.fill = {
      type: "pattern",
      pattern: "solid",
      fgColor: { argb: "EA580C" }, // Naranja para tardanzas
    };
    cellTitulo.alignment = { horizontal: "center", vertical: "middle" };
    cellTitulo.border = {
      top: { style: "medium" },
      left: { style: "medium" },
      bottom: { style: "medium" },
      right: { style: "medium" },
    };
    worksheet.getRow(1).height = 25;

    // Subtítulo
    worksheet.mergeCells("A2:G2");
    const cellSubtitulo = worksheet.getCell("A2");
    cellSubtitulo.value = `REPORTE DE TARDANZAS CONSECUTIVAS - ${nivelTexto}`;
    cellSubtitulo.font = { size: 14, bold: true, color: { argb: "FFFFFF" } };
    cellSubtitulo.fill = {
      type: "pattern",
      pattern: "solid",
      fgColor: { argb: "F97316" },
    };
    cellSubtitulo.alignment = { horizontal: "center", vertical: "middle" };
    cellSubtitulo.border = {
      top: { style: "medium" },
      left: { style: "medium" },
      bottom: { style: "medium" },
      right: { style: "medium" },
    };
    worksheet.getRow(2).height = 20;

    // Info configuración
    worksheet.mergeCells("A3:D3");
    const cellConfig = worksheet.getCell("A3");
    cellConfig.value = `⏰ Umbral: ${config.tardanzasConsecutivasMaximas} tardanzas | Tolerancia: ${config.toleranciaTardanzaMinutos} min | Hora inicio: ${config.horaInicioClases}`;
    cellConfig.font = { size: 11, bold: true, color: { argb: "EA580C" } };
    cellConfig.fill = {
      type: "pattern",
      pattern: "solid",
      fgColor: { argb: "FFEDD5" },
    };
    cellConfig.alignment = { horizontal: "center", vertical: "middle" };
    cellConfig.border = {
      top: { style: "thin" },
      left: { style: "medium" },
      bottom: { style: "thin" },
      right: { style: "thin" },
    };

    worksheet.mergeCells("E3:G3");
    const cellFecha = worksheet.getCell("E3");
    cellFecha.value = `Fecha de reporte: ${new Date().toLocaleDateString(
      "es-PE"
    )}`;
    cellFecha.font = { size: 11, italic: true };
    cellFecha.alignment = { horizontal: "center", vertical: "middle" };
    cellFecha.border = {
      top: { style: "thin" },
      left: { style: "thin" },
      bottom: { style: "thin" },
      right: { style: "medium" },
    };
    worksheet.getRow(3).height = 20;

    worksheet.getRow(4).height = 8;

    // ===== TABLA DE DATOS =====
    const headers = [
      "Grado",
      "Sección",
      "Apellidos y Nombres",
      "Fechas",
      "Horas de Llegada",
      "Total Días",
      "Estado",
    ];
    const headerRow = worksheet.getRow(5);

    headers.forEach((header, idx) => {
      const cell = headerRow.getCell(idx + 1);
      cell.value = header;
      cell.font = { bold: true, size: 11, color: { argb: "FFFFFF" } };
      cell.fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: "C2410C" },
      };
      cell.alignment = {
        horizontal: "center",
        vertical: "middle",
        wrapText: true,
      };
      cell.border = {
        top: { style: "medium" },
        left: { style: "thin" },
        bottom: { style: "medium" },
        right: { style: "thin" },
      };
    });
    headerRow.height = 20;

    // Anchos de columnas
    worksheet.columns = [
      { width: 10 }, // Grado
      { width: 12 }, // Sección
      { width: 32 }, // Nombres
      { width: 22 }, // Fechas
      { width: 22 }, // Horas
      { width: 12 }, // Total
      { width: 15 }, // Estado
    ];

    let filaActual = 6;

    // Agrupar por sección
    const estudiantesPorSeccion = new Map<string, EstudianteConProblema[]>();
    for (const estudiante of estudiantesGrado) {
      const key = estudiante.seccion;
      const lista = estudiantesPorSeccion.get(key) || [];
      lista.push(estudiante);
      estudiantesPorSeccion.set(key, lista);
    }

    const seccionesOrdenadas = Array.from(estudiantesPorSeccion.keys()).sort();

    for (const seccion of seccionesOrdenadas) {
      const estudiantesSeccion = estudiantesPorSeccion.get(seccion)!;

      // Cabecera de sección
      worksheet.mergeCells(`A${filaActual}:G${filaActual}`);
      const cellSeccion = worksheet.getCell(`A${filaActual}`);
      cellSeccion.value = `SECCIÓN "${seccion}"`;
      cellSeccion.font = { bold: true, size: 11, color: { argb: "FFFFFF" } };

      const colorAula = estudiantesSeccion[0].colorAula;
      cellSeccion.fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: colorAula.replace("#", "") },
      };
      cellSeccion.alignment = { horizontal: "center", vertical: "middle" };
      cellSeccion.border = {
        top: { style: "medium" },
        left: { style: "medium" },
        bottom: { style: "thin" },
        right: { style: "medium" },
      };
      worksheet.getRow(filaActual).height = 18;
      filaActual++;

      // Datos de estudiantes
      for (const estudiante of estudiantesSeccion) {
        const row = worksheet.getRow(filaActual);

        const fechasStr = estudiante.diasConsecutivos
          .map((d) => formatearFecha(d.fecha))
          .join("\n");

        const horasStr = estudiante.diasConsecutivos
          .map((d) => d.horaLlegada || "")
          .join("\n");

        row.getCell(1).value = estudiante.grado;
        row.getCell(2).value = estudiante.seccion;
        row.getCell(3).value = `${estudiante.apellidos}, ${estudiante.nombres}`;
        row.getCell(4).value = fechasStr;
        row.getCell(5).value = horasStr;
        row.getCell(6).value = estudiante.diasConsecutivos.length;
        row.getCell(7).value = "⚠️ ALERTA";

        // Estilos
        [1, 2, 6].forEach((col) => {
          row.getCell(col).alignment = {
            horizontal: "center",
            vertical: "middle",
          };
        });
        row.getCell(3).alignment = {
          horizontal: "left",
          vertical: "middle",
          indent: 1,
        };
        [4, 5].forEach((col) => {
          row.getCell(col).alignment = {
            horizontal: "center",
            vertical: "top",
            wrapText: true,
          };
        });
        row.getCell(7).font = { bold: true, color: { argb: "EA580C" } };
        row.getCell(7).alignment = { horizontal: "center", vertical: "middle" };

        const bgColor = filaActual % 2 === 0 ? "FFFFFF" : "FFF7ED";
        [1, 2, 3, 4, 5, 6, 7].forEach((col) => {
          const cell = row.getCell(col);
          cell.fill = {
            type: "pattern",
            pattern: "solid",
            fgColor: { argb: bgColor },
          };
          cell.border = {
            top: { style: "thin" },
            left: col === 1 ? { style: "medium" } : { style: "thin" },
            bottom: { style: "thin" },
            right: col === 7 ? { style: "medium" } : { style: "thin" },
          };
        });

        row.height = Math.max(25, estudiante.diasConsecutivos.length * 15);
        filaActual++;
      }

      worksheet.getRow(filaActual).height = 5;
      filaActual++;
    }

    // Resumen
    filaActual++;
    worksheet.mergeCells(`A${filaActual}:G${filaActual}`);
    const cellResumen = worksheet.getCell(`A${filaActual}`);
    cellResumen.value = `Total de estudiantes con tardanzas consecutivas: ${estudiantesGrado.length}`;
    cellResumen.font = { bold: true, size: 12, color: { argb: "FFFFFF" } };
    cellResumen.fill = {
      type: "pattern",
      pattern: "solid",
      fgColor: { argb: "EA580C" },
    };
    cellResumen.alignment = { horizontal: "center", vertical: "middle" };
    cellResumen.border = {
      top: { style: "medium" },
      left: { style: "medium" },
      bottom: { style: "medium" },
      right: { style: "medium" },
    };
    worksheet.getRow(filaActual).height = 22;
  }

  const buffer = await workbook.xlsx.writeBuffer();
  return Buffer.from(buffer);
}

/**
 * Formatea una fecha del formato DD-MM-YYYY a DD-MM para mostrar en Excel
 */
function formatearFecha(fecha: string): string {
  // Fecha viene en formato DD-MM-YYYY
  const [dia, mes] = fecha.split("-");
  return `${dia}-${mes}`;
}

// Continúa en el siguiente mensaje...
