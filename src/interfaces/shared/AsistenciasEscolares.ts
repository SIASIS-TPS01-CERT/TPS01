// Interfaz para el registro existente en MongoDB
export interface RegistroAsistenciaExistente {
  _id: string;
  Id_Estudiante: string;
  Mes: number;
  Asistencias_Mensuales: string;
}
