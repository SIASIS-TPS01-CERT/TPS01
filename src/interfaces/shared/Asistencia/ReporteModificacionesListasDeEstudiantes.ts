import { NOMBRE_ARCHIVO_LISTA_ESTUDIANTES } from "../../../constants/NOMBRE_ARCHIVOS_SISTEMA";
import { NivelEducativo } from "../NivelEducativo";

export interface ReporteActualizacionDeListasEstudiantes {
  EstadoDeListasDeEstudiantes: Record<NOMBRE_ARCHIVO_LISTA_ESTUDIANTES, Date>;
  Fecha_Actualizacion: Date;
}

export const NOMBRE_CLAVE_GOOGLE_DRIVE_IDs_LISTAS_ASISTENCIAS_ESCOLARES_HOY =
  "Google_Drive_IDs_Listas_Asistencias_Escolares_Hoy";

export interface GoogleDriveIDsListasAsistenciasEscolaresHoy {
                                  // GRADO : ID DE GOOGLE DRIVE
  [NivelEducativo.PRIMARIA]?: Record<number, string>;
  [NivelEducativo.SECUNDARIA]?: Record<number, string>;
}
