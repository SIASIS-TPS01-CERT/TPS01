import { NivelEducativo } from "../NivelEducativo";
import { RDP03_Nombres_Tablas } from "./RDP03_Tablas";

export const TABLAS_ASISTENCIAS_ESCOLARES: Record<
  NivelEducativo,
  Record<number, RDP03_Nombres_Tablas>
> = {
  [NivelEducativo.PRIMARIA]: {
    1: "T_A_E_P_1",
    2: "T_A_E_P_2",
    3: "T_A_E_P_3",
    4: "T_A_E_P_4",
    5: "T_A_E_P_5",
    6: "T_A_E_P_6",
  },
  [NivelEducativo.SECUNDARIA]: {
    1: "T_A_E_S_1",
    2: "T_A_E_S_2",
    3: "T_A_E_S_3",
    4: "T_A_E_S_4",
    5: "T_A_E_S_5",
  },
};
