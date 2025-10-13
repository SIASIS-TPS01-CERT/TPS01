import { NivelEducativo } from "../interfaces/shared/NivelEducativo";
import { GradosPrimaria, GradosSecundaria } from "./GRADOS_POR_NIVEL_EDUCATIVO";

export const NOMBRE_ARCHIVO_CON_DATOS_ASISTENCIA_DIARIOS =
  "datos-asistencia-hoy-ie20935.json";

// RELACIONADO A LOS ARCHIVOS DE LISTAS DE ESTUDIANTES DE SECUNDARIA Y
// PRIMARIA A GUARDAR EN BLOBS Y GOOGLE DRIVE
export type GradoPorNivel<N extends NivelEducativo> =
  N extends NivelEducativo.PRIMARIA
    ? GradosPrimaria
    : N extends NivelEducativo.SECUNDARIA
    ? GradosSecundaria
    : never;

export type NOMBRE_ARCHIVO_LISTA_ESTUDIANTES<
  N extends NivelEducativo = NivelEducativo
> = N extends NivelEducativo ? `Estudiantes_${N}_${GradoPorNivel<N>}` : never;

export const NOMBRES_ARCHIVOS_LISTAS_ESTUDIANTES_DIARIAS: Record<
  NivelEducativo,
  Record<number, NOMBRE_ARCHIVO_LISTA_ESTUDIANTES>
> = {
  [NivelEducativo.PRIMARIA]: {
    1: "Estudiantes_P_1",
    2: "Estudiantes_P_2",
    3: "Estudiantes_P_3",
    4: "Estudiantes_P_4",
    5: "Estudiantes_P_5",
    6: "Estudiantes_P_6",
  },
  [NivelEducativo.SECUNDARIA]: {
    1: "Estudiantes_S_1",
    2: "Estudiantes_S_2",
    3: "Estudiantes_S_3",
    4: "Estudiantes_S_4",
    5: "Estudiantes_S_5",
  },
};

export const NOMBRE_ARCHIVO_REPORTE_ACTUALIZACION_DE_LISTAS_DE_ESTUDIANTES =
  "reporte-actualizacion-listas-estudiantes.json";

export const NOMBRE_ARCHIVO_IDS_ARCHIVOS_ULTIMAS_ASISTENCIAS_DIARIAS =
  "ids-archivos-ultimas-asistencias-escolares-diarias.json";
