import { T_Aulas } from "@prisma/client";
import {
  GradosPrimaria,
  GradosSecundaria,
} from "../../../../../constants/GRADOS_POR_NIVEL_EDUCATIVO";
import { NivelEducativo } from "../../../../../interfaces/shared/NivelEducativo";
import { MongoOperation } from "../../../../../interfaces/shared/RDP03/MongoOperation";
import RDP03_DB_INSTANCES from "../../../connectors/mongodb";

/**
 * Obtiene las aulas de un nivel y grado específico
 */
export async function obtenerAulasPorGradoYNivel<T extends NivelEducativo>(
  nivel: T,
  grado: T extends NivelEducativo.PRIMARIA ? GradosPrimaria : GradosSecundaria
): Promise<T_Aulas[]> {
  try {
    const operation: MongoOperation = {
      operation: "find",
      collection: "T_Aulas",
      filter: {
        Nivel: nivel,
        Grado: grado,
      },
      options: {
        projection: {
          Id_Aula: "$_id",
          Nivel: 1,
          Grado: 1,
          Seccion: 1,
          Color: 1,
          Id_Profesor_Primaria: 1,
          Id_Profesor_Secundaria: 1,
          _id: 0,
        },
        sort: { Seccion: 1 },
      },
    };

    const result = await RDP03_DB_INSTANCES.executeOperation(operation);
    return result || [];
  } catch (error) {
    console.error(`Error al obtener aulas de ${nivel} grado ${grado}:`, error);
    return [];
  }
}
