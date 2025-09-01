import { NOMBRE_ARCHIVO_LISTA_ESTUDIANTES } from "../../../constants/NOMBRE_ARCHIVOS_SISTEMA";

export interface ReporteActualizacionDeListasEstudiantes {
  EstadoDeListasDeEstudiantes: Record<NOMBRE_ARCHIVO_LISTA_ESTUDIANTES, Date>;
  Fecha_Actualizacion: Date;
}
