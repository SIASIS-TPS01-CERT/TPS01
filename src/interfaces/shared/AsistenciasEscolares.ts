import { ModoRegistro } from "./ModoRegistroPersonal";

// Interfaz para el registro existente en MongoDB
export interface RegistroAsistenciaExistente {
  _id: string;
  Id_Estudiante: string;
  Mes: number;
  Asistencias_Mensuales: string;
}

export interface DetalleAsistenciaEscolar {
  DesfaseSegundos: number | null;
}

export type AsistenciaEscolarDeUnDia = {
  [ModoRegistro.Entrada]: DetalleAsistenciaEscolar | null;
  [ModoRegistro.Salida]?: DetalleAsistenciaEscolar | null;
};
