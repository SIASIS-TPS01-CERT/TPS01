import { Entorno } from "../interfaces/shared/Entornos";
import { RDP03 } from "../interfaces/shared/RDP03Instancias";
import { ENTORNO } from "./ENTORNO";
import "dotenv/config";

// Definición de las instancias de RDP03
export const DIRECTIVO_INSTANCES =
  ENTORNO === Entorno.PRODUCCION ? [RDP03.INS1] : [RDP03.INS1];
export const AUXILIAR_INSTANCES =
  ENTORNO === Entorno.PRODUCCION ? [RDP03.INS2] : [RDP03.INS2];
export const PERSONAL_ADMIN_INSTANCES =
  ENTORNO === Entorno.PRODUCCION ? [RDP03.INS2] : [RDP03.INS2];

export const PROFESOR_SECUNDARIA_INSTANCES =
  ENTORNO === Entorno.PRODUCCION ? [RDP03.INS3] : [RDP03.INS3];
export const TUTOR_INSTANCES =
  ENTORNO === Entorno.PRODUCCION ? [RDP03.INS3] : [RDP03.INS3];

export const PROFESOR_PRIMARIA_INSTANCES =
  ENTORNO === Entorno.PRODUCCION ? [RDP03.INS4] : [RDP03.INS4];

export const RESPONSABLE_INSTANCES =
  ENTORNO === Entorno.PRODUCCION
    ? [RDP03.INS1, RDP03.INS2, RDP03.INS3, RDP03.INS4, RDP03.INS5]
    : [RDP03.INS1, RDP03.INS2, RDP03.INS3, RDP03.INS4, RDP03.INS5];

    
export const RDP03_INSTANCES_DATABASE_URL_MAP = new Map<
  RDP03,
  string | undefined
>([
  [RDP03.INS1, process.env.RDP03_INS1_DATABASE_URL],
  [RDP03.INS2, process.env.RDP03_INS2_DATABASE_URL],
  [RDP03.INS3, process.env.RDP03_INS3_DATABASE_URL],
  [RDP03.INS4, process.env.RDP03_INS4_DATABASE_URL],
  [RDP03.INS5, process.env.RDP03_INS5_DATABASE_URL],
  // Aquí se pueden agregar más instancias fácilmente en el futuro
]);

