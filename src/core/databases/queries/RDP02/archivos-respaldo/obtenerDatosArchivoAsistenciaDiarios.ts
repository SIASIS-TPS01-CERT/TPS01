import { NOMBRE_ARCHIVO_CON_DATOS_ASISTENCIA_DIARIOS } from "../../../../../constants/NOMBRE_ARCHIVOS_SISTEMA";
import { DatosAsistenciaHoyIE20935 } from "../../../../../interfaces/shared/Asistencia/DatosAsistenciaHoyIE20935";
import { RolesSistema } from "../../../../../interfaces/shared/RolesSistema";
import RDP02_DB_INSTANCES from "../../../connectors/postgres";
import { PersonalActivo } from "../personales-para-toma-asistencia/verificarYRegistrarAsistenciasIncompletas";

export async function obtenerUltimoArchivoAsistencia(): Promise<string> {
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

  // Auxiliares
  datosAsistencia.ListaDeAuxiliares.forEach((auxiliar) => {
    personalActivo.push({
      dni: auxiliar.DNI_Auxiliar,
      rol: RolesSistema.Auxiliar,
      tablaMensualEntrada: "T_Control_Entrada_Mensual_Auxiliar",
      tablaMensualSalida: "T_Control_Salida_Mensual_Auxiliar",
      campoId: "Id_C_E_M_P_Auxiliar",
      campoDNI: "DNI_Auxiliar",
      nombreCompleto: `${auxiliar.Nombres} ${auxiliar.Apellidos}`,
    });
  });

  // Profesores Primaria
  datosAsistencia.ListaDeProfesoresPrimaria.forEach((profesor) => {
    personalActivo.push({
      dni: profesor.DNI_Profesor_Primaria,
      rol: RolesSistema.ProfesorPrimaria,
      tablaMensualEntrada: "T_Control_Entrada_Mensual_Profesores_Primaria",
      tablaMensualSalida: "T_Control_Salida_Mensual_Profesores_Primaria",
      campoId: "Id_C_E_M_P_Profesores_Primaria",
      campoDNI: "DNI_Profesor_Primaria",
      nombreCompleto: `${profesor.Nombres} ${profesor.Apellidos}`,
    });
  });

  // Profesores Secundaria
  datosAsistencia.ListaDeProfesoresSecundaria.forEach((profesor) => {
    personalActivo.push({
      dni: profesor.DNI_Profesor_Secundaria,
      rol: RolesSistema.ProfesorSecundaria,
      tablaMensualEntrada: "T_Control_Entrada_Mensual_Profesores_Secundaria",
      tablaMensualSalida: "T_Control_Salida_Mensual_Profesores_Secundaria",
      campoId: "Id_C_E_M_P_Profesores_Secundaria",
      campoDNI: "DNI_Profesor_Secundaria",
      nombreCompleto: `${profesor.Nombres} ${profesor.Apellidos}`,
      horaEntradaEsperada: String(profesor.Hora_Entrada_Dia_Actual),
      horaSalidaEsperada: String(profesor.Hora_Salida_Dia_Actual),
    });
  });

  // Personal Administrativo
  datosAsistencia.ListaDePersonalesAdministrativos.forEach((personal) => {
    personalActivo.push({
      dni: personal.DNI_Personal_Administrativo,
      rol: RolesSistema.PersonalAdministrativo,
      tablaMensualEntrada: "T_Control_Entrada_Mensual_Personal_Administrativo",
      tablaMensualSalida: "T_Control_Salida_Mensual_Personal_Administrativo",
      campoId: "Id_C_E_M_P_Administrativo",
      campoDNI: "DNI_Personal_Administrativo",
      nombreCompleto: `${personal.Nombres} ${personal.Apellidos}`,
      horaEntradaEsperada: String(personal.Horario_Laboral_Entrada),
      horaSalidaEsperada: String(personal.Horario_Laboral_Salida),
    });
  });

  return personalActivo;
}
