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

// 🆕 NUEVAS IMPORTACIONES PARA VALIDACIONES
import { obtenerVacacionesInterescolares } from "../../core/databases/queries/RDP02/vacaciones-interescolares/obtenerVacacionesInterescolares";
import { obtenerSemanaDeGestion } from "../../core/databases/queries/RDP02/fechas-importantes/obtenerSemanaDeGestion";
import { obtenerFechasAñoEscolar } from "../../core/databases/queries/RDP02/fechas-importantes/obtenerFechasAñoEscolar";
import verificarFueraAñoEscolar from "../../core/databases/queries/RDP02/fechas-importantes/verificarDentroAñoEscolar";
import { verificarDentroSemanaGestion } from "../../core/databases/queries/RDP02/fechas-importantes/verificarDentroSemanaGestion";
import { procesarYGuardarAsistenciasDiarias } from "../../core/utils/helpers/processors/procesarYGuadarAsistenciasDiarias";
import { T_Aulas } from "@prisma/client";

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
  seccion: string; // 🆕 AGREGADO
  nivel: NivelEducativo;
  tablaAsistencia: RDP03_Nombres_Tablas;
  nombreCompleto: string;
  // 🆕 NUEVO: Información del aula
  aula: T_Aulas;
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
 * 🆕 Verifica si estamos en un periodo especial (vacaciones o semana de gestión)
 */
function verificarPeriodoEspecial(
  fechaLocal: Date,
  vacaciones: Array<{ Inicio: Date; Fin: Date }>,
  semanaGestion: { Inicio: Date; Fin: Date } | null
): { esVacaciones: boolean; esSemanaGestion: boolean } {
  // Verificar vacaciones
  const esVacaciones = vacaciones.some((vacacion) => {
    return fechaLocal >= vacacion.Inicio && fechaLocal <= vacacion.Fin;
  });

  // Verificar semana de gestión
  const esSemanaGestion = semanaGestion
    ? verificarDentroSemanaGestion(fechaLocal, semanaGestion)
    : false;

  return { esVacaciones, esSemanaGestion: Boolean(esSemanaGestion) };
}

/**
 * Función principal del script
 */
async function main() {
  // Variables para almacenar datos en memoria
  let estudiantesActivos: EstudianteActivoSecundaria[] = [];
  let registrosFiltrados: RegistroEstudianteSecundariaRedis[] = [];
  let fechaLocalPeru: Date;

  try {
    console.log(
      "🚀 Iniciando procesamiento de asistencias de estudiantes de secundaria..."
    );

    // =================================================================
    // 🆕 VALIDACIONES PREVIAS - SI ALGUNA FALLA, NO SE EJECUTA NADA
    // =================================================================

    // Obtener fecha actual
    const fechas = obtenerFechasActuales();
    fechaLocalPeru = fechas.fechaLocalPeru;
    console.log(
      `\n📅 Fecha actual (Perú): ${fechaLocalPeru.toISOString().split("T")[0]}`
    );

    console.log("\n🔍 === VALIDACIONES PREVIAS ===");

    // ✅ VALIDACIÓN 1: ¿Es día de evento?
    console.log("1️⃣ Verificando si es día de evento...");
    const esDiaEvento = await verificarDiaEvento(fechaLocalPeru);

    if (esDiaEvento) {
      console.log("🎉 ❌ ES DÍA DE EVENTO");
      console.log(
        "⛔ Script cancelado: No se procesa asistencia en días de evento"
      );
      return; // ← SALIR COMPLETAMENTE
    }
    console.log("   ✅ No es día de evento, continuando...");

    // ✅ VALIDACIÓN 2: ¿Estamos fuera del año escolar?
    console.log("2️⃣ Verificando si estamos dentro del año escolar...");
    const fechasAñoEscolar = await obtenerFechasAñoEscolar();
    const fueraAñoEscolar = verificarFueraAñoEscolar(
      fechaLocalPeru,
      fechasAñoEscolar.Inicio_Año_Escolar,
      fechasAñoEscolar.Fin_Año_Escolar
    );

    if (fueraAñoEscolar) {
      console.log("📅 ❌ FUERA DEL AÑO ESCOLAR");
      console.log(
        `   Inicio año escolar: ${
          fechasAñoEscolar.Inicio_Año_Escolar.toISOString().split("T")[0]
        }`
      );
      console.log(
        `   Fin año escolar: ${
          fechasAñoEscolar.Fin_Año_Escolar.toISOString().split("T")[0]
        }`
      );
      console.log(
        "⛔ Script cancelado: No se procesa asistencia fuera del año escolar"
      );
      return; // ← SALIR COMPLETAMENTE
    }
    console.log("   ✅ Dentro del año escolar, continuando...");

    // ✅ VALIDACIÓN 3: ¿Estamos en vacaciones interescolares?
    console.log("3️⃣ Verificando si estamos en vacaciones interescolares...");
    const vacacionesInterescolares = await obtenerVacacionesInterescolares();
    const semanaGestion = await obtenerSemanaDeGestion();

    const { esVacaciones, esSemanaGestion } = verificarPeriodoEspecial(
      fechaLocalPeru,
      vacacionesInterescolares,
      semanaGestion
    );

    if (esVacaciones) {
      console.log("🏖️ ❌ ESTAMOS EN VACACIONES INTERESCOLARES");
      console.log(
        "⛔ Script cancelado: No se procesa asistencia en vacaciones"
      );
      return; // ← SALIR COMPLETAMENTE
    }
    console.log("   ✅ No estamos en vacaciones, continuando...");

    // ✅ VALIDACIÓN 4: ¿Estamos en semana de gestión?
    console.log("4️⃣ Verificando si estamos en semana de gestión...");

    if (esSemanaGestion) {
      console.log("📋 ❌ ESTAMOS EN SEMANA DE GESTIÓN");
      if (semanaGestion) {
        console.log(
          `   Inicio: ${semanaGestion.Inicio.toISOString().split("T")[0]}`
        );
        console.log(`   Fin: ${semanaGestion.Fin.toISOString().split("T")[0]}`);
      }
      console.log(
        "⛔ Script cancelado: No se procesa asistencia en semana de gestión"
      );
      return; // ← SALIR COMPLETAMENTE
    }
    console.log("   ✅ No estamos en semana de gestión, continuando...");

    console.log("\n✅ === TODAS LAS VALIDACIONES PASADAS ===");
    console.log("🚦 Procediendo con el procesamiento de asistencias...\n");

    // =================================================================
    // PROCESAMIENTO NORMAL - Solo se ejecuta si pasó todas las validaciones
    // =================================================================

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
      // FASE 1: Procesamiento de registros Redis
      console.log(
        "\n🔄 === FASE 1: Procesamiento de registros Redis de estudiantes de secundaria ==="
      );

      // 1.1 Obtener registros desde Redis
      const registrosRedis =
        await obtenerRegistrosAsistenciaEstudiantesSecundariaRedis();

      // 1.2 Filtrar registros de salida si no está habilitado
      registrosFiltrados = registrosRedis;
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
      console.log(
        "\n📋 === FASE 2: Registrar faltas de estudiantes de secundaria ==="
      );

      // 2.1 Obtener estudiantes activos de secundaria
      estudiantesActivos = await obtenerEstudiantesActivosSecundaria();
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

    // =================================================================
    // FASE 3: Procesar y guardar archivo de asistencias diarias
    // IMPORTANTE: Esto se hace DESPUÉS de desbloquear roles
    // =================================================================
    console.log(
      "\n📦 === FASE 3: Procesar y guardar archivo de asistencias diarias ==="
    );

    try {
      await procesarYGuardarAsistenciasDiarias({
        estudiantesActivos,
        registrosRedis: registrosFiltrados,
        nivel: NivelEducativo.SECUNDARIA,
        fechaActual: fechaLocalPeru,
      });
    } catch (error) {
      console.error(
        "❌ Error procesando archivo de asistencias diarias:",
        error
      );
      // No lanzamos el error para que el script pueda finalizar correctamente
    }

    console.log("\n✅ === PROCESO COMPLETO FINALIZADO EXITOSAMENTE ===");
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
