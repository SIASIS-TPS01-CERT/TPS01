import {
  GradosPrimaria,
  GradosSecundaria,
} from "../../../../constants/GRADOS_POR_NIVEL_EDUCATIVO";
import { NivelEducativo } from "../../../../interfaces/shared/NivelEducativo";

/**
 * Genera el nombre del archivo para una combinación de nivel y grado
 */
export function generarNombreArchivo<T extends NivelEducativo>(
  nivel: T,
  grado: T extends NivelEducativo.PRIMARIA ? GradosPrimaria : GradosSecundaria
): string {
  return `Estudiantes_${nivel}_${grado}.json`;
}
