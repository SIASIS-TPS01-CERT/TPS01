import { T_Aulas, T_Estudiantes } from "@prisma/client";
import { NivelEducativo } from "../NivelEducativo";
import {
  GradosPrimaria,
  GradosSecundaria,
} from "../../../constants/GRADOS_POR_NIVEL_EDUCATIVO";

export interface ListaEstudiantesPorGradoParaHoy<T extends NivelEducativo> {
  ListaEstudiantes: T_Estudiantes[];
  Aulas: T_Aulas[];
  Nivel: T;
  Grado: T extends NivelEducativo.PRIMARIA ? GradosPrimaria : GradosSecundaria;
  Fecha_Actualizacion: Date;
  Fecha_Actualizacion_Peru: Date;
}
