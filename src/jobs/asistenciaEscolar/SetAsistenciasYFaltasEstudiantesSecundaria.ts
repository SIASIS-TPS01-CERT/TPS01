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
import { T_Aulas, T_Vacaciones_Interescolares } from "@prisma/client";
import { normalizarFecha } from "../../core/utils/helpers/formatters/normalizarFechasSinHoras";

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
  vacaciones: T_Vacaciones_Interescolares[],
  semanaGestion: { Inicio: Date; Fin: Date } | null
): { esVacaciones: boolean; esSemanaGestion: boolean } {
  // Normalizar la fecha local (establecer hora a 00:00:00)
  const fechaLocalNormalizada = normalizarFecha(fechaLocal);

  // Verificar vacaciones
  const esVacaciones = vacaciones.some((vacacion) => {
    const inicioVacaciones = normalizarFecha(new Date(vacacion.Fecha_Inicio));
    const finVacaciones = normalizarFecha(new Date(vacacion.Fecha_Conclusion));

    return (
      fechaLocalNormalizada >= inicioVacaciones &&
      fechaLocalNormalizada <= finVacaciones
    );
  });

  // Verificar semana de gestión
  const esSemanaGestion = semanaGestion
    ? verificarDentroSemanaGestion(fechaLocalNormalizada, {
        Inicio: normalizarFecha(semanaGestion.Inicio),
        Fin: normalizarFecha(semanaGestion.Fin),
      })
    : false;

  return { esVacaciones, esSemanaGestion: Boolean(esSemanaGestion) };
}
// Códigos de salida
const EXIT_CODES = {
  SUCCESS: 0, // Éxito: Se procesaron asistencias
  SKIPPED: 2, // Cancelado: Validaciones previas (día evento, vacaciones, etc.)
  ERROR: 1, // Error: Fallo técnico
};

async function main() {
  let estudiantesActivos: EstudianteActivoSecundaria[] = [];
  let registrosFiltrados: RegistroEstudianteSecundariaRedis[] = [];
  let fechaLocalPeru: Date;

  try {
    console.log(
      "🚀 Iniciando procesamiento de asistencias de estudiantes de secundaria..."
    );

    // =================================================================
    // VALIDACIONES PREVIAS
    // =================================================================
    const fechas = obtenerFechasActuales();
    fechaLocalPeru = fechas.fechaLocalPeru;
    console.log(
      `\n📅 Fecha actual (Perú): ${fechaLocalPeru.toISOString().split("T")[0]}`
    );

    console.log("\n🔍 === VALIDACIONES PREVIAS ===");

    // VALIDACIÓN 1: ¿Es día de evento?
    console.log("1️⃣ Verificando si es día de evento...");
    const esDiaEvento = await verificarDiaEvento(fechaLocalPeru);

    if (esDiaEvento) {
      console.log("🎉 ❌ ES DÍA DE EVENTO");
      console.log(
        "⛔ Script cancelado: No se procesa asistencia en días de evento"
      );
      await Promise.all([closePool(), closeClient()]);
      console.log("🔌 Conexiones cerradas. Finalizando con código SKIPPED...");
      process.exit(EXIT_CODES.SKIPPED); // ← Código 2: Cancelación válida
    }
    console.log("   ✅ No es día de evento, continuando...");

    // VALIDACIÓN 2: ¿Estamos fuera del año escolar?
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
      await Promise.all([closePool(), closeClient()]);
      console.log("🔌 Conexiones cerradas. Finalizando con código SKIPPED...");
      process.exit(EXIT_CODES.SKIPPED); // ← Código 2: Cancelación válida
    }
    console.log("   ✅ Dentro del año escolar, continuando...");

    // VALIDACIÓN 3: ¿Estamos en vacaciones interescolares?
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
      await Promise.all([closePool(), closeClient()]);
      console.log("🔌 Conexiones cerradas. Finalizando con código SKIPPED...");
      process.exit(EXIT_CODES.SKIPPED); // ← Código 2: Cancelación válida
    }
    console.log("   ✅ No estamos en vacaciones, continuando...");

    // VALIDACIÓN 4: ¿Estamos en semana de gestión?
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
      await Promise.all([closePool(), closeClient()]);
      console.log("🔌 Conexiones cerradas. Finalizando con código SKIPPED...");
      process.exit(EXIT_CODES.SKIPPED); // ← Código 2: Cancelación válida
    }
    console.log("   ✅ No estamos en semana de gestión, continuando...");

    console.log("\n✅ === TODAS LAS VALIDACIONES PASADAS ===");
    console.log("🚦 Procediendo con el procesamiento de asistencias...\n");

    // =================================================================
    // PROCESAMIENTO NORMAL - Solo se ejecuta si pasó todas las validaciones
    // =================================================================

    const rolesABloquear = [
      RolesSistema.Directivo,
      RolesSistema.Auxiliar,
      RolesSistema.ProfesorSecundaria,
      RolesSistema.Tutor,
      RolesSistema.Responsable,
    ];

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

      const registrosRedis =
        await obtenerRegistrosAsistenciaEstudiantesSecundariaRedis();
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

      // FASE 2: Registrar faltas
      console.log(
        "\n📋 === FASE 2: Registrar faltas de estudiantes de secundaria ==="
      );

      estudiantesActivos = await obtenerEstudiantesActivosSecundaria();
      console.log(
        `👥 Estudiantes activos de secundaria encontrados: ${estudiantesActivos.length}`
      );

      const resultado = await registrarFaltasEstudiantesSecundaria(
        estudiantesActivos,
        registrosFiltrados,
        fechaLocalPeru
      );

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
        "\n🎉 Proceso de asistencias de estudiantes de secundaria completado exitosamente"
      );
    } finally {
      try {
        await desbloquearRoles(rolesABloquear);
        console.log("🔓 Roles desbloqueados correctamente");
      } catch (unlockError) {
        console.warn("⚠️ Error al desbloquear roles:", unlockError);
      }
    }

    // FASE 3: Procesar y guardar archivo de asistencias diarias
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
    }

    console.log("\n✅ === PROCESO COMPLETO FINALIZADO EXITOSAMENTE ===");

    // ← Éxito real: Se procesaron asistencias
    process.exit(EXIT_CODES.SUCCESS);
  } catch (error) {
    console.error(
      "❌ Error en procesamiento de asistencias de estudiantes de secundaria:",
      error
    );
    await Promise.all([closePool(), closeClient()]).catch(() => {});
    process.exit(EXIT_CODES.ERROR); // ← Código 1: Error técnico
  } finally {
    try {
      await Promise.all([closePool(), closeClient()]);
      console.log("🔌 Conexiones cerradas. Finalizando proceso...");
    } catch (closeError) {
      console.error("❌ Error al cerrar conexiones:", closeError);
    }
  }
}

// Ejecutar el script
main();
