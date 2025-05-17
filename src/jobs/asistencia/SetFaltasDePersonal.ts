import { closePool } from "../../core/databases/connectors/postgres";
import {
  obtenerPersonalActivoDesdeJSON,
  obtenerUltimoArchivoAsistencia,
} from "../../core/databases/queries/RDP02/archivos-respaldo/obtenerDatosArchivoAsistenciaDiarios";
import { bloquearRoles } from "../../core/databases/queries/RDP02/bloqueo-roles/bloquearRoles";
import { desbloquearRoles } from "../../core/databases/queries/RDP02/bloqueo-roles/desbloquearRoles";
import { verificarYRegistrarAsistenciasIncompletas } from "../../core/databases/queries/RDP02/personales-para-toma-asistencia/verificarYRegistrarAsistenciasIncompletas";
import { descargarArchivoDatosAsistenciaDesdeGoogleDrive } from "../../core/external/google/drive/descargarArchivoDatosAsistencia";
import { obtenerFechasActuales } from "../../core/utils/dates/obtenerFechasActuales";
import { RolesSistema } from "../../interfaces/shared/RolesSistema";

async function main() {
  try {
    console.log("Iniciando verificación de asistencias incompletas...");

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
    } catch (blockError) {
      console.warn(
        "No se pudieron bloquear todos los roles, continuando de todos modos:",
        blockError
      );
    }

    try {
      // Obtener fecha actual en Perú
      const { fechaLocalPeru } = obtenerFechasActuales();

      // 1. Obtener el ID del último archivo de asistencia
      const googleDriveId = await obtenerUltimoArchivoAsistencia();
      console.log(
        `ID del último archivo de asistencia encontrado: ${googleDriveId}`
      );

      // 2. Descargar el archivo de asistencia
      const datosAsistencia =
        await descargarArchivoDatosAsistenciaDesdeGoogleDrive(googleDriveId);
      console.log("Datos de asistencia descargados correctamente");

      // 3. Extraer lista de personal activo del archivo
      const personalActivo = await obtenerPersonalActivoDesdeJSON(
        datosAsistencia
      );
      console.log(`Personal activo encontrado: ${personalActivo.length}`);

      // 4. Verificar y registrar asistencias incompletas
      const resultado = await verificarYRegistrarAsistenciasIncompletas(
        personalActivo,
        fechaLocalPeru
      );

      // 5. Mostrar resultados
      console.log("=== Resultados de registro de asistencias incompletas ===");
      console.log(`Total personal activo procesado: ${personalActivo.length}`);
      console.log(
        `Registros de entrada creados: ${resultado.registrosEntradaCreados}`
      );
      console.log(
        `Registros de salida creados: ${resultado.registrosSalidaCreados}`
      );

      // Detallar personal sin registro de entrada
      console.log("\nPersonal sin registro de entrada:");
      if (resultado.personalSinRegistroEntrada.length === 0) {
        console.log("Ninguno (o no se pudieron procesar por falta de tablas)");
      } else {
        resultado.personalSinRegistroEntrada.forEach((persona) => {
          console.log(
            `- ${persona.nombreCompleto} (${persona.dni}) - ${persona.rol}`
          );
        });
      }

      // Detallar personal sin registro de salida
      console.log("\nPersonal sin registro de salida:");
      if (resultado.personalSinRegistroSalida.length === 0) {
        console.log("Ninguno (o no se pudieron procesar por falta de tablas)");
      } else {
        resultado.personalSinRegistroSalida.forEach((persona) => {
          console.log(
            `- ${persona.nombreCompleto} (${persona.dni}) - ${persona.rol}`
          );
        });
      }

      console.log("\nProceso completado exitosamente.");
    } finally {
      // Desbloquear todos los roles sin importar lo que suceda
      try {
        await desbloquearRoles(todosLosRoles);
        
      } catch (unlockError) {
        console.warn("Error al desbloquear roles:", unlockError);
      }
    }
  } catch (error) {
    console.error(
      "Error en el proceso de verificación de asistencias incompletas:",
      error
    );
    process.exit(1);
  } finally {
    try {
      await closePool();
      console.log("Conexiones cerradas. Finalizando proceso...");
    } catch (poolError) {
      console.error("Error al cerrar el pool de conexiones:", poolError);
    }
    process.exit(0);
  }
}

main();
