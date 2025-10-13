import { closePool } from "../../core/databases/connectors/postgres";
import {
  obtenerPersonalActivoDesdeJSON,
  obtenerUltimoArchivoDatosAsistenciaHoy,
} from "../../core/databases/queries/RDP02/archivos-respaldo/obtenerDatosArchivoAsistenciaDiarios";
import { registrarAsistenciasUnitariasDePersonalDesdeRedis } from "../../core/databases/queries/RDP02/asistencias-diarias-unitarias/registrarAsistenciasUnitariasDePersonalDesdeRedis";
import { bloquearRoles } from "../../core/databases/queries/RDP02/bloqueo-roles/bloquearRoles";
import { desbloquearRoles } from "../../core/databases/queries/RDP02/bloqueo-roles/desbloquearRoles";
import { verificarYRegistrarAsistenciasIncompletas } from "../../core/databases/queries/RDP02/personal-en-general/verificarYRegistrarAsistenciasIncompletas";
import { obtenerRegistrosAsistenciaPersonalRedis } from "../../core/databases/queries/RDP05/obtenerRegistrosAsistenciaPersonalRedis";
import { descargarArchivoJSONDesdeGoogleDrive } from "../../core/external/google/drive/descargarArchivoJSONDesdeGoogle";
import { obtenerFechasActuales } from "../../core/utils/dates/obtenerFechasActuales";
import { RolesSistema } from "../../interfaces/shared/RolesSistema";
import { verificarDiaEvento } from "../../core/databases/queries/RDP02/eventos/verificarDiaEvento";
import { DatosAsistenciaHoyIE20935 } from "../../interfaces/shared/Asistencia/DatosAsistenciaHoyIE20935";

// ========================================================
// FUNCIÓN PRINCIPAL
// ========================================================

async function main() {
  try {
    console.log(
      "🚀 Iniciando verificación de asistencias incompletas y procesamiento de registros Redis..."
    );

    // Definir todos los roles que vamos a bloquear
    const todosLosRoles = [
      RolesSistema.Directivo,
      RolesSistema.ProfesorPrimaria,
      RolesSistema.Auxiliar,
      RolesSistema.ProfesorSecundaria,
      RolesSistema.Tutor,
      RolesSistema.Responsable,
      RolesSistema.PersonalAdministrativo,
    ];

    // Bloquear todos los roles al inicio
    try {
      await bloquearRoles(todosLosRoles);
      console.log("🔒 Roles bloqueados correctamente");
    } catch (blockError) {
      console.warn(
        "⚠️  No se pudieron bloquear todos los roles, continuando de todos modos:",
        blockError
      );
    }

    try {
      // Obtener fecha actual en Perú
      const { fechaLocalPeru } = obtenerFechasActuales();
      console.log(
        `📅 Procesando asistencias para la fecha: ${
          fechaLocalPeru.toISOString().split("T")[0]
        }`
      );

      // 🎯 NUEVA VERIFICACIÓN: Verificar si es día de evento
      const esDiaEvento = await verificarDiaEvento(fechaLocalPeru);
      console.log(`🎉 ¿Es día de evento?: ${esDiaEvento ? "SÍ" : "NO"}`);

      // ========================================================
      // FASE 1: Procesamiento de registros Redis
      // ========================================================
      console.log(
        "\n🔄 === FASE 1: Procesamiento de registros Redis de personal ==="
      );

      // 1.1 Obtener registros de personal desde Redis
      const registrosPersonalRedis =
        await obtenerRegistrosAsistenciaPersonalRedis();

      // 1.2 Persistir registros en la base de datos
      if (registrosPersonalRedis.length > 0) {
        console.log(
          `🔄 Procesando ${registrosPersonalRedis.length} registros de asistencia de personal...`
        );

        await registrarAsistenciasUnitariasDePersonalDesdeRedis(
          registrosPersonalRedis
        );

        console.log("✅ Registros de Redis procesados correctamente.");
        console.log(
          "⏰ Los registros en Redis expirarán automáticamente según su configuración."
        );
      } else {
        console.log(
          "ℹ️  No se encontraron registros de personal en Redis para procesar"
        );
      }

      // ========================================================
      // FASE 2: Verificación de asistencias incompletas
      // ========================================================
      if (esDiaEvento) {
        console.log("\n🎉 === OMITIENDO FASE 2: Es día de evento ===");
        console.log(
          "🚫 No se registrarán faltas porque es un día de evento (feriado, celebración, etc.)"
        );
        console.log(
          "📝 En días de evento no hay clases, por lo que no se considera falta del personal"
        );
      } else {
        console.log(
          "\n📋 === FASE 2: Verificación de asistencias incompletas ==="
        );

        // 2.1. Obtener el ID del último archivo de asistencia
        const googleDriveId = await obtenerUltimoArchivoDatosAsistenciaHoy();
        console.log(
          `🗂️  ID del último archivo de asistencia encontrado: ${googleDriveId}`
        );

        // 2.2. Descargar el archivo de asistencia
        const datosAsistencia =
          await descargarArchivoJSONDesdeGoogleDrive<DatosAsistenciaHoyIE20935>(
            googleDriveId
          );
        console.log("📥 Datos de asistencia descargados correctamente");

        // 2.3. Extraer lista de personal activo del archivo
        const personalActivo = await obtenerPersonalActivoDesdeJSON(
          datosAsistencia
        );
        console.log(`👥 Personal activo encontrado: ${personalActivo.length}`);

        // 2.4. Verificar y registrar asistencias incompletas
        const resultado = await verificarYRegistrarAsistenciasIncompletas(
          personalActivo,
          fechaLocalPeru
        );

        // 2.5. Mostrar resultados
        console.log(
          "\n📊 === Resultados de registro de asistencias incompletas ==="
        );
        console.log(
          `👥 Total personal activo procesado: ${personalActivo.length}`
        );
        console.log(
          `📥 Registros de entrada creados: ${resultado.registrosEntradaCreados}`
        );
        console.log(
          `📤 Registros de salida creados: ${resultado.registrosSalidaCreados}`
        );

        // Detallar personal sin registro de entrada
        console.log("\n📥 Personal sin registro de entrada:");
        if (resultado.personalSinRegistroEntrada.length === 0) {
          console.log(
            "✅ Ninguno (o no se pudieron procesar por falta de tablas)"
          );
        } else {
          resultado.personalSinRegistroEntrada.forEach((persona) => {
            console.log(
              `❌ ${persona.nombreCompleto} (${persona.idUsuario}) - ${persona.rol}`
            );
          });
        }

        // Detallar personal sin registro de salida
        console.log("\n📤 Personal sin registro de salida:");
        if (resultado.personalSinRegistroSalida.length === 0) {
          console.log(
            "✅ Ninguno (o no se pudieron procesar por falta de tablas)"
          );
        } else {
          resultado.personalSinRegistroSalida.forEach((persona) => {
            console.log(
              `❌ ${persona.nombreCompleto} (${persona.idUsuario}) - ${persona.rol}`
            );
          });
        }
      }

      console.log("\n🎉 Proceso completado exitosamente.");
    } finally {
      // Desbloquear todos los roles sin importar lo que suceda
      try {
        await desbloquearRoles(todosLosRoles);
        console.log("🔓 Roles desbloqueados correctamente");
      } catch (unlockError) {
        console.warn("⚠️  Error al desbloquear roles:", unlockError);
      }
    }
  } catch (error) {
    console.error(
      "❌ Error en el proceso de verificación de asistencias incompletas:",
      error
    );
    process.exit(1);
  } finally {
    try {
      await closePool();
      console.log("🔌 Conexiones cerradas. Finalizando proceso...");
    } catch (poolError) {
      console.error("❌ Error al cerrar el pool de conexiones:", poolError);
    }
    process.exit(0);
  }
}

main();
