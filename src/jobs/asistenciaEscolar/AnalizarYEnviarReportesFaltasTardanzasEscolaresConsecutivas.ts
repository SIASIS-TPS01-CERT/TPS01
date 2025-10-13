
import { closeClient } from "../../core/databases/connectors/mongodb";
import { closePool } from "../../core/databases/connectors/postgres";
import { NivelEducativo } from "../../interfaces/shared/NivelEducativo";

import { verificarArchivosAsistenciaEscolarDisponibles } from "../../core/utils/helpers/verificators/verificarArchivosAsistenciaEscolarDisponibles";
import { obtenerConfiguracionesReportesEscolares } from "../../core/databases/queries/RDP02/ajustes-generales/obtenerConfiguracionesReportesEscolares";
import { analizarAsistenciasEscolaresConsecutivas } from "../../core/utils/helpers/analizers/analizarAsistenciasEscolaresConsecutivas";
import { generarReportesExcel } from "../../core/utils/helpers/generators/generarReportesAlertaAsistenciaEscolarEnExcel";
import { obtenerDestinatariosCorreos } from "../../core/databases/queries/RDP02/personal-en-general/obtenerCorreosPersonalAutorizadoAlertasAsistenciasEscolares";
import { enviarCorreosReportes } from "../../core/utils/helpers/mailers/enviarCorreoReporteAlertaAsistenciasEscolares";

/**
 * Script principal para analizar asistencias y enviar reportes
 * Parámetros:
 * - S: Secundaria
 * - P: Primaria
 */
async function main() {
  try {
    // ============================================================
    // PASO 1: Validar parámetro de nivel educativo
    // ============================================================
    const nivelParam = process.argv[2];

    if (!nivelParam || !["S", "P"].includes(nivelParam.toUpperCase())) {
      console.error(
        "❌ Error: Debe especificar el nivel educativo (S para Secundaria, P para Primaria)"
      );
      console.error(
        "   Uso: npx ts-node AnalizarYEnviarReportesAsistencia.ts S"
      );
      process.exit(1);
    }

    const nivel: NivelEducativo = nivelParam.toUpperCase() as NivelEducativo;
    const nivelTexto =
      nivel === NivelEducativo.SECUNDARIA ? "SECUNDARIA" : "PRIMARIA";

    console.log("\n" + "=".repeat(70));
    console.log(
      `🎯 INICIANDO ANÁLISIS Y REPORTES DE ASISTENCIA - ${nivelTexto}`
    );
    console.log("=".repeat(70) + "\n");

    // ============================================================
    // PASO 2: Obtener configuraciones desde la base de datos
    // ============================================================
    console.log("⚙️  PASO 1: Obteniendo configuraciones del sistema...\n");

    const configuraciones = await obtenerConfiguracionesReportesEscolares(
      nivel
    );

    console.log(`   📧 Correos para faltas ${nivelTexto}:`);
    console.log(
      `      - Directivos: ${
        configuraciones.enviarCorreoFaltasDirectivos
          ? "✅ Activado"
          : "❌ Desactivado"
      }`
    );

    if (nivel === NivelEducativo.SECUNDARIA) {
      console.log(
        `      - Tutores: ${
          configuraciones.enviarCorreoFaltasTutores
            ? "✅ Activado"
            : "❌ Desactivado"
        }`
      );
      console.log(
        `      - Auxiliares: ${
          configuraciones.enviarCorreoFaltasAuxiliares
            ? "✅ Activado"
            : "❌ Desactivado"
        }`
      );
    } else {
      console.log(
        `      - Profesores: ${
          configuraciones.enviarCorreoFaltasProfesores
            ? "✅ Activado"
            : "❌ Desactivado"
        }`
      );
    }

    console.log(`\n   📧 Correos para tardanzas ${nivelTexto}:`);
    console.log(
      `      - Directivos: ${
        configuraciones.enviarCorreoTardanzasDirectivos
          ? "✅ Activado"
          : "❌ Desactivado"
      }`
    );

    if (nivel === NivelEducativo.SECUNDARIA) {
      console.log(
        `      - Tutores: ${
          configuraciones.enviarCorreoTardanzasTutores
            ? "✅ Activado"
            : "❌ Desactivado"
        }`
      );
      console.log(
        `      - Auxiliares: ${
          configuraciones.enviarCorreoTardanzasAuxiliares
            ? "✅ Activado"
            : "❌ Desactivado"
        }`
      );
    } else {
      console.log(
        `      - Profesores: ${
          configuraciones.enviarCorreoTardanzasProfesores
            ? "✅ Activado"
            : "❌ Desactivado"
        }`
      );
    }

    console.log(`\n   ⚙️  Umbrales configurados:`);
    console.log(
      `      - Faltas consecutivas máximas: ${configuraciones.faltasConsecutivasMaximas}`
    );
    console.log(
      `      - Tardanzas consecutivas máximas: ${configuraciones.tardanzasConsecutivasMaximas}`
    );
    console.log(
      `      - Tolerancia tardanza: ${configuraciones.toleranciaTardanzaMinutos} minutos`
    );
    console.log(
      `      - Hora inicio clases: ${configuraciones.horaInicioClases}`
    );

    // ============================================================
    // PASO 3: Verificar si hay configuraciones activas
    // ============================================================
    const hayConfiguracionFaltasActiva =
      configuraciones.enviarCorreoFaltasDirectivos ||
      (nivel === NivelEducativo.SECUNDARIA
        ? configuraciones.enviarCorreoFaltasTutores ||
          configuraciones.enviarCorreoFaltasAuxiliares
        : configuraciones.enviarCorreoFaltasProfesores);

    const hayConfiguracionTardanzasActiva =
      configuraciones.enviarCorreoTardanzasDirectivos ||
      (nivel === NivelEducativo.SECUNDARIA
        ? configuraciones.enviarCorreoTardanzasTutores ||
          configuraciones.enviarCorreoTardanzasAuxiliares
        : configuraciones.enviarCorreoTardanzasProfesores);

    if (!hayConfiguracionFaltasActiva && !hayConfiguracionTardanzasActiva) {
      console.log(
        "\n⚠️  No hay configuraciones activas para envío de correos."
      );
      console.log("   El script finalizará sin realizar análisis.\n");
      return;
    }

    console.log(
      "\n✅ PASO 1 COMPLETADO: Configuraciones obtenidas correctamente\n"
    );

    // ============================================================
    // PASO 4: Verificar archivos disponibles
    // ============================================================
    console.log(
      "📁 PASO 2: Verificando archivos de asistencia disponibles...\n"
    );

    const {
      archivosDisponibles,
      suficientesParaFaltas,
      suficientesParaTardanzas,
    } = await verificarArchivosAsistenciaEscolarDisponibles(
      nivel,
      configuraciones.faltasConsecutivasMaximas,
      configuraciones.tardanzasConsecutivasMaximas
    );

    console.log(`   📊 Archivos disponibles: ${archivosDisponibles.length}`);
    console.log(
      `   ✓ Suficientes para faltas (${
        configuraciones.faltasConsecutivasMaximas
      }): ${suficientesParaFaltas ? "✅ Sí" : "❌ No"}`
    );
    console.log(
      `   ✓ Suficientes para tardanzas (${
        configuraciones.tardanzasConsecutivasMaximas
      }): ${suficientesParaTardanzas ? "✅ Sí" : "❌ No"}`
    );

    const analizarFaltas =
      hayConfiguracionFaltasActiva && suficientesParaFaltas;
    const analizarTardanzas =
      hayConfiguracionTardanzasActiva && suficientesParaTardanzas;

    if (!analizarFaltas && !analizarTardanzas) {
      console.log(
        "\n⚠️  No hay suficientes archivos para realizar ningún análisis."
      );
      console.log("   El script finalizará sin generar reportes.\n");
      return;
    }

    console.log("\n✅ PASO 2 COMPLETADO: Archivos verificados correctamente\n");

    // ============================================================
    // PASO 5: Analizar asistencias consecutivas
    // ============================================================
    console.log("🔍 PASO 3: Analizando asistencias consecutivas...\n");

    const resultadosAnalisis = await analizarAsistenciasEscolaresConsecutivas(
      nivel,
      archivosDisponibles,
      configuraciones,
      analizarFaltas,
      analizarTardanzas
    );

    console.log(
      `   📉 Estudiantes con faltas consecutivas: ${resultadosAnalisis.estudiantesConFaltas.length}`
    );
    console.log(
      `   ⏰ Estudiantes con tardanzas consecutivas: ${resultadosAnalisis.estudiantesConTardanzas.length}`
    );

    if (
      resultadosAnalisis.estudiantesConFaltas.length === 0 &&
      resultadosAnalisis.estudiantesConTardanzas.length === 0
    ) {
      console.log(
        "\n✅ ¡Excelente! No hay estudiantes con faltas o tardanzas consecutivas."
      );
      console.log("   No se generarán reportes ni se enviarán correos.\n");
      return;
    }

    console.log("\n✅ PASO 3 COMPLETADO: Análisis finalizado correctamente\n");

    // ============================================================
    // PASO 6: Generar reportes en Excel
    // ============================================================
    console.log("📊 PASO 4: Generando reportes en Excel...\n");

    const reportesExcel = await generarReportesExcel(
      nivel,
      resultadosAnalisis,
      configuraciones
    );

    console.log(`   📄 Reportes generados:`);
    if (reportesExcel.reporteFaltas) {
      console.log(
        `      ✅ Reporte de faltas (${(
          reportesExcel.reporteFaltas.completo.byteLength / 1024
        ).toFixed(2)} KB)`
      );
    }
    if (reportesExcel.reporteTardanzas) {
      console.log(
        `      ✅ Reporte de tardanzas (${(
          reportesExcel.reporteTardanzas.completo.byteLength / 1024
        ).toFixed(2)} KB)`
      );
    }
    if (reportesExcel.reportesPorAula.size > 0) {
      console.log(
        `      ✅ ${reportesExcel.reportesPorAula.size} reportes individuales por aula`
      );
    }

    console.log(
      "\n✅ PASO 4 COMPLETADO: Reportes Excel generados correctamente\n"
    );

    // ============================================================
    // PASO 7: Obtener destinatarios de correos
    // ============================================================
    console.log("👥 PASO 5: Obteniendo destinatarios de correos...\n");

    const destinatarios = await obtenerDestinatariosCorreos(
      nivel,
      configuraciones,
      resultadosAnalisis.aulasAfectadas
    );

    const totalDestinatarios =
      destinatarios.directivos.length +
      destinatarios.tutores.length +
      destinatarios.auxiliares.length +
      destinatarios.profesoresPrimaria.length;

    console.log(`   📧 Destinatarios encontrados: ${totalDestinatarios}`);
    console.log(`      - Directivos: ${destinatarios.directivos.length}`);

    if (nivel === NivelEducativo.SECUNDARIA) {
      console.log(`      - Tutores: ${destinatarios.tutores.length}`);
      console.log(`      - Auxiliares: ${destinatarios.auxiliares.length}`);
    } else {
      console.log(
        `      - Profesores: ${destinatarios.profesoresPrimaria.length}`
      );
    }

    console.log(
      "\n✅ PASO 5 COMPLETADO: Destinatarios obtenidos correctamente\n"
    );

    // ============================================================
    // PASO 8: Enviar correos con reportes
    // ============================================================
    console.log("📧 PASO 6: Enviando correos con reportes...\n");

    const resultadoEnvio = await enviarCorreosReportes(
      nivel,
      configuraciones,
      reportesExcel,
      destinatarios,
      analizarFaltas,
      analizarTardanzas
    );

    console.log(`   📨 Resumen de envíos:`);
    console.log(
      `      ✅ Correos enviados exitosamente: ${resultadoEnvio.exitosos}`
    );
    if (resultadoEnvio.fallidos > 0) {
      console.log(`      ❌ Correos fallidos: ${resultadoEnvio.fallidos}`);
    }
    console.log(
      `      ⏭️  Destinatarios omitidos (sin correo): ${resultadoEnvio.omitidos}`
    );

    console.log("\n✅ PASO 6 COMPLETADO: Correos enviados correctamente\n");

    // ============================================================
    // RESUMEN FINAL
    // ============================================================
    console.log("\n" + "=".repeat(70));
    console.log("✅ PROCESO COMPLETADO EXITOSAMENTE");
    console.log("=".repeat(70));
    console.log(`\n📊 Resumen del proceso:`);
    console.log(`   • Nivel educativo: ${nivelTexto}`);
    console.log(`   • Archivos analizados: ${archivosDisponibles.length}`);
    console.log(
      `   • Estudiantes con faltas: ${resultadosAnalisis.estudiantesConFaltas.length}`
    );
    console.log(
      `   • Estudiantes con tardanzas: ${resultadosAnalisis.estudiantesConTardanzas.length}`
    );
    console.log(
      `   • Reportes generados: ${reportesExcel.reporteFaltas ? 1 : 0} + ${
        reportesExcel.reporteTardanzas ? 1 : 0
      }`
    );
    console.log(`   • Correos enviados: ${resultadoEnvio.exitosos}`);
    console.log("");
  } catch (error) {
    console.error("\n❌ ERROR CRÍTICO EN EL PROCESO:");
    console.error(error);
    process.exit(1);
  } finally {
    try {
      await Promise.all([closePool(), closeClient()]);
      console.log("🔌 Conexiones cerradas correctamente");
    } catch (closeError) {
      console.error("❌ Error al cerrar conexiones:", closeError);
    }
    process.exit(0);
  }
}

// Ejecutar el script
main();
