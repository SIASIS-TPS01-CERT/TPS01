import { T_Estudiantes } from "@prisma/client";
import {
  GradosPrimaria,
  GradosSecundaria,
} from "../../../../../constants/GRADOS_POR_NIVEL_EDUCATIVO";
import { NivelEducativo } from "../../../../../interfaces/shared/NivelEducativo";
import { MongoOperation } from "../../../../../interfaces/shared/RDP03/MongoOperation";
import RDP03_DB_INSTANCES from "../../../connectors/mongodb";


// Tipado condicional para los grados según el nivel
export type GradosPorNivel<T extends NivelEducativo> =
  T extends NivelEducativo.PRIMARIA
    ? GradosPrimaria
    : T extends NivelEducativo.SECUNDARIA
    ? GradosSecundaria
    : never;

/**
 * Obtiene todos los estudiantes de un grado y nivel específico
 * @param nivel - Nivel educativo (PRIMARIA o SECUNDARIA)
 * @param grado - Grado específico del nivel (tipado condicionalmente)
 * @returns Array de estudiantes (T_Estudiantes[]) - vacío si no hay estudiantes
 */
export async function obtenerEstudiantesPorGradoYNivel<
  T extends NivelEducativo
>(
  nivel: T,
  grado: GradosPorNivel<T>
): Promise<T_Estudiantes[]> {
  try {
    const operation: MongoOperation = {
      operation: "aggregate",
      collection: "T_Estudiantes",
      pipeline: [
        // Unir con la colección de aulas
        {
          $lookup: {
            from: "T_Aulas",
            localField: "Id_Aula",
            foreignField: "_id",
            as: "aula",
          },
        },
        // Filtrar por nivel, grado y estado activo
        {
          $match: {
            Estado: true, // Solo estudiantes activos
            "aula.Nivel": nivel,
            "aula.Grado": grado,
          },
        },
        // Proyectar solo los campos necesarios del estudiante
        {
          $project: {
            Id_Estudiante: "$_id",
            Nombres: 1,
            Apellidos: 1,
            Estado: 1,
            Google_Drive_Foto_ID: 1,
            Id_Aula: 1,
            _id: 0,
          },
        },
        // Ordenar alfabéticamente por apellidos y nombres
        {
          $sort: {
            Apellidos: 1,
            Nombres: 1,
          },
        },
      ],
    };

    // Ejecutar la operación de LECTURA en UNA sola instancia aleatoria
    const result = await RDP03_DB_INSTANCES.executeMongoDBOperation(operation, {
      //   role: rolConsulta, NO ESPECIFICAMOS ROL PARA USAR CUALQUIER INSTANCIA DE LAS 5
      useCache: true, // Usar cache para optimizar consultas frecuentes
    });

    // Retornar array vacío si no hay resultados
    return result || [];
  } catch (error) {
    console.error(
      `Error al obtener estudiantes de ${nivel} grado ${grado}:`,
      error
    );

    // En caso de error, retornar array vacío en lugar de lanzar excepción
    return [];
  }
}
