import { RDP03_Nombres_Tablas } from "../interfaces/shared/RDP03/RDP03_Tablas";

export interface ModificacionEspecificaDetallesRDP03 {
  Nombre_Tabla: RDP03_Nombres_Tablas;
  //Las modificaciones especificas se agruparan por
  //estos campos especificados aqui ⬇️⬇️⬇️⬇️
  Campos_Identificadores: string[];

  Tabla_Adicional: RDP03_Nombres_Tablas;
  Campo_Enlace: string;
}

export const ModificacionesEspecificasEstudiantesAgrupadasPorNivelYGrado: ModificacionEspecificaDetallesRDP03 =
  {
    Nombre_Tabla: "T_Estudiantes",
    Campos_Identificadores: ["Nivel", "Grado"],
    Tabla_Adicional: "T_Aulas",
    Campo_Enlace: "Id_Aula",
  };
