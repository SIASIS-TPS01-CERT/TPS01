import { closeClient } from "../../core/databases/connectors/mongodb";
import { closePool } from "../../core/databases/connectors/postgres";
import { bloquearRoles } from "../../core/databases/queries/RDP02/bloqueo-roles/bloquearRoles";
import { desbloquearRoles } from "../../core/databases/queries/RDP02/bloqueo-roles/desbloquearRoles";
import { verificarDiaEvento } from "../../core/databases/queries/RDP02/eventos/verificarDiaEvento";
import { obtenerFechasActuales } from "../../core/utils/dates/obtenerFechasActuales";
import { RolesSistema } from "../../interfaces/shared/RolesSistema";
import { TipoAsistencia } from "../../interfaces/shared/AsistenciaRequests";
import { redisClient } from "../../config/Redis/RedisClient";
import { CONTROL_ASISTENCIA_DE_SALIDA_SECUNDARIA } from "../../constants/ASISTENCIA_ENTRADA_SALIDA_ESCOLAR";
import { obtenerEstudiantesActivosSecundaria } from "../../core/databases/queries/RDP04/estudiantes/obtenerEstudiantesActivosSecundaria";
import { registrarAsistenciasEstudiantesSecundariaDesdeRedis } from "../../core/databases/queries/RDP03/asistencias-escolares/registrarAsistenciasEstudiantesSecundariaDesdeRedis";
import { RDP03_Nombres_Tablas } from "../../interfaces/shared/RDP03/RDP03_Tablas";
import { registrarFaltasEstudiantesSecundaria } from "../../core/databases/queries/RDP03/asistencias-escolares/registrarFaltasEstudiantesSecundaria";
import { NivelEducativo } from "../../interfaces/shared/NivelEducativo";
import { ModoRegistro } from "../../interfaces/shared/ModoRegistroPersonal";
import { ActoresSistema } from "../../interfaces/shared/ActoresSistema";

// Interfaz para registros de estudiantes desde Redis
export interface RegistroEstudianteSecundariaRedis {
  fecha: string;
  modoRegistro: ModoRegistro;
  actor: ActoresSistema.Estudiante;
  nivel: NivelEducativo.SECUNDARIA;
  grado: number;
  seccion: string;
  idEstudiante: string;
  desfaseSegundos: number;
  clave: string;
}

// Interfaz para estudiante activo
export interface EstudianteActivoSecundaria {
  idEstudiante: string;
  nombres: string;
  apellidos: string;
  grado: number;
  nivel: NivelEducativo;
  tablaAsistencia: RDP03_Nombres_Tablas;
  nombreCompleto: string;
}

/**
 * Obtiene registros de asistencia de estudiantes de secundaria desde Redis
 */
async function obtenerRegistrosAsistenciaEstudiantesSecundariaRedis(): Promise<
  RegistroEstudianteSecundariaRedis[]
> {
  try {
    console.log(
      "🔍 Obteniendo registros de estudiantes de secundaria desde Redis..."
    );

    const redisClientInstance = redisClient(
      TipoAsistencia.ParaEstudiantesSecundaria
    );
    const keys = await redisClientInstance.keys("*");

    console.log(
      `🔑 Encontradas ${keys.length} claves en Redis para estudiantes de secundaria`
    );

    if (keys.length === 0) {
      return [];
    }

    const registros: RegistroEstudianteSecundariaRedis[] = [];

    // Procesar cada clave
    for (const clave of keys) {
      try {
        // Estructura de clave: 2025-08-29:E:E:S:1:A:77742971
        const partesClav = clave.split(":");

        if (partesClav.length !== 7) {
          console.warn(`⚠️ Clave con formato inválido ignorada: ${clave}`);
          continue;
        }

        const [
          fecha,
          modoRegistro,
          actor,
          nivel,
          grado,
          seccion,
          idEstudiante,
        ] = partesClav;

        // Validar que es secundaria
        if (nivel !== "S" || actor !== "E") {
          continue;
        }

        // Obtener valor desde Redis
        const valor = await redisClientInstance.get(clave);
        if (!valor || !Array.isArray(valor) || valor.length === 0) {
          console.warn(`⚠️ Valor inválido para clave ${clave}: ${valor}`);
          continue;
        }

        const desfaseSegundos = parseInt(valor[0], 10);
        if (isNaN(desfaseSegundos)) {
          console.warn(`⚠️ Desfase inválido para clave ${clave}: ${valor[0]}`);
          continue;
        }

        registros.push({
          fecha,
          modoRegistro: modoRegistro as ModoRegistro,
          actor: actor as ActoresSistema.Estudiante,
          nivel: nivel as NivelEducativo.SECUNDARIA,
          grado: parseInt(grado, 10),
          seccion,
          idEstudiante,
          desfaseSegundos,
          clave,
        });
      } catch (error) {
        console.error(`❌ Error procesando clave ${clave}:`, error);
      }
    }

    console.log(
      `✅ Se procesaron ${registros.length} registros válidos de estudiantes de secundaria`
    );
    return registros;
  } catch (error) {
    console.error(
      "❌ Error obteniendo registros de Redis para estudiantes de secundaria:",
      error
    );
    return [];
  }
}

/**
 * Función principal del script
 */
async function main() {
  try {
    console.log(
      "🚀 Iniciando procesamiento de asistencias de estudiantes de secundaria..."
    );

    // Definir roles a bloquear (solo los que pueden interactuar con estudiantes)
    const rolesABloquear = [
      RolesSistema.Directivo,
      RolesSistema.Auxiliar,
      RolesSistema.ProfesorSecundaria,
      RolesSistema.Tutor,
      RolesSistema.Responsable,
    ];

    // Bloquear roles al inicio
    try {
      await bloquearRoles(rolesABloquear);
      console.log("🔒 Roles bloqueados correctamente");
    } catch (blockError) {
      console.warn(
        "⚠️ No se pudieron bloquear todos los roles, continuando:",
        blockError
      );
    }

    try {
      // Obtener fecha actual
      const { fechaLocalPeru } = obtenerFechasActuales();
      console.log(
        `📅 Procesando asistencias de secundaria para: ${
          fechaLocalPeru.toISOString().split("T")[0]
        }`
      );

      // Verificar si es día de evento
      const esDiaEvento = await verificarDiaEvento(fechaLocalPeru);
      console.log(`🎉 ¿Es día de evento?: ${esDiaEvento ? "SÍ" : "NO"}`);

      // FASE 1: Procesamiento de registros Redis
      console.log(
        "\n🔄 === FASE 1: Procesamiento de registros Redis de estudiantes de secundaria ==="
      );

      // 1.1 Obtener registros desde Redis
      const registrosRedis =
        await obtenerRegistrosAsistenciaEstudiantesSecundariaRedis();

      // 1.2 Filtrar registros de salida si no está habilitado
      let registrosFiltrados = registrosRedis;
      if (!CONTROL_ASISTENCIA_DE_SALIDA_SECUNDARIA) {
        const registrosEntradaOriginales = registrosRedis.filter(
          (r) => r.modoRegistro === ModoRegistro.Entrada
        ).length;
        const registrosSalidaOriginales = registrosRedis.filter(
          (r) => r.modoRegistro === ModoRegistro.Salida
        ).length;

        registrosFiltrados = registrosRedis.filter(
          (r) => r.modoRegistro === ModoRegistro.Entrada
        );

        console.log(
          `🚫 Control de salida deshabilitado - Ignorando ${registrosSalidaOriginales} registros de salida`
        );
        console.log(
          `✅ Procesando ${registrosEntradaOriginales} registros de entrada`
        );
      }

      // 1.3 Persistir registros en MongoDB
      if (registrosFiltrados.length > 0) {
        console.log(
          `🔄 Procesando ${registrosFiltrados.length} registros de estudiantes de secundaria...`
        );

        await registrarAsistenciasEstudiantesSecundariaDesdeRedis(
          registrosFiltrados
        );

        console.log("✅ Registros de Redis procesados correctamente");
      } else {
        console.log(
          "ℹ️ No hay registros de estudiantes de secundaria para procesar"
        );
      }

      // FASE 2: Registrar faltas para estudiantes sin registro
      if (esDiaEvento) {
        console.log("\n🎉 === OMITIENDO FASE 2: Es día de evento ===");
        console.log("🚫 No se registrarán faltas porque es un día de evento");
      } else {
        console.log(
          "\n📋 === FASE 2: Registrar faltas de estudiantes de secundaria ==="
        );

        // 2.1 Obtener estudiantes activos de secundaria
        const estudiantesActivos = await obtenerEstudiantesActivosSecundaria();
        console.log(
          `👥 Estudiantes activos de secundaria encontrados: ${estudiantesActivos.length}`
        );

        // 2.2 Registrar faltas
        const resultado = await registrarFaltasEstudiantesSecundaria(
          estudiantesActivos,
          registrosFiltrados,
          fechaLocalPeru
        );

        // 2.3 Mostrar resultados
        console.log("\n📊 === Resultados de registro de faltas ===");
        console.log(
          `👥 Total estudiantes activos procesados: ${estudiantesActivos.length}`
        );
        console.log(
          `📥 Faltas de entrada registradas: ${resultado.faltasEntradaRegistradas}`
        );

        if (CONTROL_ASISTENCIA_DE_SALIDA_SECUNDARIA) {
          console.log(
            `📤 Faltas de salida registradas: ${resultado.faltasSalidaRegistradas}`
          );
        }

        console.log(
          `❌ Estudiantes con faltas de entrada: ${resultado.estudiantesSinEntrada.length}`
        );

        if (
          CONTROL_ASISTENCIA_DE_SALIDA_SECUNDARIA &&
          resultado.estudiantesSinSalida
        ) {
          console.log(
            `❌ Estudiantes con faltas de salida: ${resultado.estudiantesSinSalida.length}`
          );
        }
      }

      console.log(
        "\n🎉 Proceso de asistencias de estudiantes de secundaria completado exitosamente"
      );
    } finally {
      // Desbloquear roles
      try {
        await desbloquearRoles(rolesABloquear);
        console.log("🔓 Roles desbloqueados correctamente");
      } catch (unlockError) {
        console.warn("⚠️ Error al desbloquear roles:", unlockError);
      }
    }
  } catch (error) {
    console.error(
      "❌ Error en procesamiento de asistencias de estudiantes de secundaria:",
      error
    );
    process.exit(1);
  } finally {
    try {
      await Promise.all([closePool(), closeClient()]);
      console.log("🔌 Conexiones cerradas. Finalizando proceso...");
    } catch (closeError) {
      console.error("❌ Error al cerrar conexiones:", closeError);
    }
    process.exit(0);
  }
}

// Ejecutar el script
main();
