import { T_Modificaciones_Especificas } from "@prisma/client";
import { ModificacionesEspecificasEstudiantesAgrupadasPorNivelYGrado } from "../../../../../constants/MODIFICACIONES_ESPECIFICAS_RDP03";
import { MongoOperation } from "../../../../../interfaces/shared/RDP03/MongoOperation";
import RDP03_DB_INSTANCES from "../../../connectors/mongodb";

/**
 * Obtiene las modificaciones específicas de estudiantes agrupadas por nivel y grado
 */
export async function obtenerModificacionesEspecificasEstudiantes(): Promise<
  T_Modificaciones_Especificas[]
> {
  try {
    const config = ModificacionesEspecificasEstudiantesAgrupadasPorNivelYGrado;
    const camposString = config.Campos_Identificadores.join(",");

    const operation: MongoOperation = {
      operation: "find",
      collection: "T_Modificaciones_Especificas",
      filter: {
        Nombre_Tabla: config.Nombre_Tabla,
        Campos_Identificadores: camposString,
      },
      options: {
        projection: {
          Id_Modificacion_Especifica: "$_id",
          Nombre_Tabla: 1,
          Campos_Identificadores: 1,
          Valores_Campos_Identificadores: 1,
          Id_Registro_Modificado: 1,
          Fecha_Modificacion: 1,
          Ultima_Operacion: 1,
          Contador_Modificaciones: 1,
          _id: 0,
        },
      },
    };

    const result = await RDP03_DB_INSTANCES.executeOperation(operation);

    console.log(
      `🔍 Encontradas ${result.length} modificaciones específicas de estudiantes`
    );
    return result || [];
  } catch (error) {
    console.error(
      "Error al obtener modificaciones específicas de estudiantes:",
      error
    );
    return [];
  }
}
