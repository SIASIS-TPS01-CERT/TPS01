// src/core/database/queries/configuracion/obtenerHorariosEscolares.ts
import { HorarioTomaAsistencia } from "../../../../../interfaces/shared/Asistencia/DatosAsistenciaHoyIE20935";
import { NivelEducativo } from "../../../../../interfaces/shared/NivelEducativo";
import { convertirStringTiempoADate } from "../../../../utils/functions/parsers/convertirStringATiempoDate";
import RDP02_DB_INSTANCES from '../../../connectors/postgres';

export async function obtenerHorariosEscolares(): Promise<
  Record<NivelEducativo, HorarioTomaAsistencia>
> {
  const fechaHoy = new Date();

  const sql = `
    SELECT "Nombre", "Valor", "Descripcion"
    FROM "T_Horarios_Asistencia"
    WHERE "Nombre" IN (
      'Hora_Inicio_Asistencia_Primaria',
      'Hora_Final_Asistencia_Primaria',
      'Hora_Inicio_Asistencia_Secundaria',
      'Hora_Final_Asistencia_Secundaria'
    )
  `;

  const result = await RDP02_DB_INSTANCES.query(sql);
  const horarios = result.rows.reduce((acc: any, row: any) => {
    acc[row.Nombre] = row.Valor;
    return acc;
  }, {});

  // Convertir los strings de tiempo a objetos Date
  return {
    [NivelEducativo.PRIMARIA]: {
      Inicio: convertirStringTiempoADate(
        fechaHoy,
        horarios.Hora_Inicio_Asistencia_Primaria
      ),
      Fin: convertirStringTiempoADate(
        fechaHoy,
        horarios.Hora_Final_Asistencia_Primaria
      ),
    },
    [NivelEducativo.SECUNDARIA]: {
      Inicio: convertirStringTiempoADate(
        fechaHoy,
        horarios.Hora_Inicio_Asistencia_Secundaria
      ),
      Fin: convertirStringTiempoADate(
        fechaHoy,
        horarios.Hora_Final_Asistencia_Secundaria
      ),
    },
  };
}
