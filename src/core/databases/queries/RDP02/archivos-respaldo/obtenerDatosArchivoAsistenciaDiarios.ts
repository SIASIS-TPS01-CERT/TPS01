import { NOMBRE_ARCHIVO_CON_DATOS_ASISTENCIA_DIARIOS } from "../../../../../constants/NOMBRE_ARCHIVOS_SISTEMA";
import { DatosAsistenciaHoyIE20935 } from "../../../../../interfaces/shared/Asistencia/DatosAsistenciaHoyIE20935";
import { RolesSistema } from "../../../../../interfaces/shared/RolesSistema";
import RDP02_DB_INSTANCES from "../../../connectors/postgres";
import { PersonalActivo } from "../personal-en-general/verificarYRegistrarAsistenciasIncompletas";

export async function obtenerUltimoArchivoDatosAsistenciaHoy(): Promise<string> {
  try {
    const sql = `
      SELECT "Google_Drive_Id"
      FROM "T_Archivos_Respaldo_Google_Drive"
      WHERE "Nombre_Archivo" LIKE '${NOMBRE_ARCHIVO_CON_DATOS_ASISTENCIA_DIARIOS}'
      ORDER BY "Ultima_Modificacion" DESC
      LIMIT 1
    `;

    const result = await RDP02_DB_INSTANCES.query(sql);

    if (result.rowCount === 0) {
      throw new Error(
        "No se encontró ningún archivo de asistencia en la base de datos"
      );
    }

    return result.rows[0].Google_Drive_Id;
  } catch (error) {
    console.error("Error al obtener último archivo de asistencia:", error);
    throw error;
  }
}

export async function obtenerPersonalActivoDesdeJSON(
  datosAsistencia: DatosAsistenciaHoyIE20935
): Promise<PersonalActivo[]> {
  const personalActivo: PersonalActivo[] = [];

  // 🆕 DIRECTIVOS - NUEVA SECCIÓN
  if (
    datosAsistencia.ListaDeDirectivos &&
    datosAsistencia.ListaDeDirectivos.length > 0
  ) {
    console.log(
      `🏢 Procesando ${datosAsistencia.ListaDeDirectivos.length} directivos del JSON...`
    );

    for (const directivo of datosAsistencia.ListaDeDirectivos) {
      // 🔄 Convertir ID → Id_Directivo para compatibilidad con BD

      personalActivo.push({
        idUsuario: String(directivo.Id_Directivo), // ⚠️ IMPORTANTE: Usar Id_Directivo como string para compatibilidad
        rol: RolesSistema.Directivo,
        tablaMensualEntrada: "T_Control_Entrada_Mensual_Directivos",
        tablaMensualSalida: "T_Control_Salida_Mensual_Directivos",
        campoId: "Id_C_E_M_P_Directivo",
        campoIdUsuario: "Id_Directivo", // Campo en BD que usa Id_Directivo
        nombreCompleto: `${directivo.Nombres} ${directivo.Apellidos}`,
        horaEntradaEsperada: String(directivo.Hora_Entrada_Dia_Actual),
        horaSalidaEsperada: String(directivo.Hora_Salida_Dia_Actual),
      });

      console.log(
        `✅ Directivo agregado: ${directivo.Nombres} ${directivo.Apellidos} (ID: ${directivo.Identificador_Nacional} → Id: ${directivo.Id_Directivo})`
      );
    }
  } else {
    console.log("ℹ️  No se encontraron directivos en ListaDeDirectivos");
  }

  // Auxiliares
  if (
    datosAsistencia.ListaDeAuxiliares &&
    datosAsistencia.ListaDeAuxiliares.length > 0
  ) {
    console.log(
      `👥 Procesando ${datosAsistencia.ListaDeAuxiliares.length} auxiliares...`
    );
    datosAsistencia.ListaDeAuxiliares.forEach((auxiliar) => {
      personalActivo.push({
        idUsuario: auxiliar.Id_Auxiliar,
        rol: RolesSistema.Auxiliar,
        tablaMensualEntrada: "T_Control_Entrada_Mensual_Auxiliar",
        tablaMensualSalida: "T_Control_Salida_Mensual_Auxiliar",
        campoId: "Id_C_E_M_P_Auxiliar",
        campoIdUsuario: "Id_Auxiliar",
        nombreCompleto: `${auxiliar.Nombres} ${auxiliar.Apellidos}`,
      });
    });
  }

  // Profesores Primaria
  if (
    datosAsistencia.ListaDeProfesoresPrimaria &&
    datosAsistencia.ListaDeProfesoresPrimaria.length > 0
  ) {
    console.log(
      `🎓 Procesando ${datosAsistencia.ListaDeProfesoresPrimaria.length} profesores de primaria...`
    );
    datosAsistencia.ListaDeProfesoresPrimaria.forEach((profesor) => {
      personalActivo.push({
        idUsuario: profesor.Id_Profesor_Primaria,
        rol: RolesSistema.ProfesorPrimaria,
        tablaMensualEntrada: "T_Control_Entrada_Mensual_Profesores_Primaria",
        tablaMensualSalida: "T_Control_Salida_Mensual_Profesores_Primaria",
        campoId: "Id_C_E_M_P_Profesores_Primaria",
        campoIdUsuario: "Id_Profesor_Primaria",
        nombreCompleto: `${profesor.Nombres} ${profesor.Apellidos}`,
      });
    });
  }

  // Profesores Secundaria
  if (
    datosAsistencia.ListaDeProfesoresSecundaria &&
    datosAsistencia.ListaDeProfesoresSecundaria.length > 0
  ) {
    console.log(
      `🏫 Procesando ${datosAsistencia.ListaDeProfesoresSecundaria.length} profesores de secundaria...`
    );
    datosAsistencia.ListaDeProfesoresSecundaria.forEach((profesor) => {
      personalActivo.push({
        idUsuario: profesor.Id_Profesor_Secundaria,
        rol: RolesSistema.ProfesorSecundaria,
        tablaMensualEntrada: "T_Control_Entrada_Mensual_Profesores_Secundaria",
        tablaMensualSalida: "T_Control_Salida_Mensual_Profesores_Secundaria",
        campoId: "Id_C_E_M_P_Profesores_Secundaria",
        campoIdUsuario: "Id_Profesor_Secundaria",
        nombreCompleto: `${profesor.Nombres} ${profesor.Apellidos}`,
        horaEntradaEsperada: String(profesor.Hora_Entrada_Dia_Actual),
        horaSalidaEsperada: String(profesor.Hora_Salida_Dia_Actual),
      });
    });
  }

  // Personal Administrativo
  if (
    datosAsistencia.ListaDePersonalesAdministrativos &&
    datosAsistencia.ListaDePersonalesAdministrativos.length > 0
  ) {
    console.log(
      `💼 Procesando ${datosAsistencia.ListaDePersonalesAdministrativos.length} personal administrativo...`
    );
    datosAsistencia.ListaDePersonalesAdministrativos.forEach((personal) => {
      personalActivo.push({
        idUsuario: personal.Id_Personal_Administrativo,
        rol: RolesSistema.PersonalAdministrativo,
        tablaMensualEntrada:
          "T_Control_Entrada_Mensual_Personal_Administrativo",
        tablaMensualSalida: "T_Control_Salida_Mensual_Personal_Administrativo",
        campoId: "Id_C_E_M_P_Administrativo",
        campoIdUsuario: "Id_Personal_Administrativo",
        nombreCompleto: `${personal.Nombres} ${personal.Apellidos}`,
        horaEntradaEsperada: String(personal.Hora_Entrada_Dia_Actual),
        horaSalidaEsperada: String(personal.Hora_Salida_Dia_Actual),
      });
    });
  }

  console.log("\n=== 📊 Resumen de personal activo extraído del JSON ===");
  console.log(
    `🏢 Directivos: ${
      personalActivo.filter((p) => p.rol === RolesSistema.Directivo).length
    }`
  );
  console.log(
    `👥 Auxiliares: ${
      personalActivo.filter((p) => p.rol === RolesSistema.Auxiliar).length
    }`
  );
  console.log(
    `🎓 Profesores Primaria: ${
      personalActivo.filter((p) => p.rol === RolesSistema.ProfesorPrimaria)
        .length
    }`
  );
  console.log(
    `🏫 Profesores Secundaria: ${
      personalActivo.filter((p) => p.rol === RolesSistema.ProfesorSecundaria)
        .length
    }`
  );
  console.log(
    `💼 Personal Administrativo: ${
      personalActivo.filter(
        (p) => p.rol === RolesSistema.PersonalAdministrativo
      ).length
    }`
  );
  console.log(`📊 Total personal activo: ${personalActivo.length}`);

  return personalActivo;
}
