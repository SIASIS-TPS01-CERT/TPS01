import { Fecha_ISO_8601 } from "../Fechas";
import { NivelEducativo } from "../NivelEducativo";

export type ArchivoIdsArchivosUltimasAsistenciasEscolaresDiarias = Record<
  NivelEducativo,
  Record<Fecha_ISO_8601, string>
>;
