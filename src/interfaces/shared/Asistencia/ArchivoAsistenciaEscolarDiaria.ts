import { T_Aulas } from "@prisma/client";
import { NivelEducativoTextos } from "../../../assets/NivelEducativoTextos";
import { AsistenciaEscolarDeUnDia } from "../AsistenciasEscolares";
import { NivelEducativo } from "../NivelEducativo";
import { ListaAsistenciasEscolaresHoy } from "./ListasAsistenciasEscolaresHoy";

export function obtenerNombreArchivoAsistenciaEscolarDiaria(
  fecha: Date,
  nivelEducativo: NivelEducativo
): string {
  return `Asistencia_Escolar_Diaria_${NivelEducativoTextos[nivelEducativo]}_${
    fecha.toISOString().split("T")[0]
  }.json`;
}



export type ArchivoAsistenciaEscolarDiaria = Record<
  string,  //Grado
  Record<
    string, //Seccion
    {
      Aula: T_Aulas;
      ListaAsistenciasEscolares: Record<string, AsistenciaEscolarDeUnDia>;
    }
  >
>;
