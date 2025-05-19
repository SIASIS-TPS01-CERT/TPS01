import { Entorno } from "../interfaces/shared/Entornos";
import "dotenv/config";

export const ENTORNO =
  process.env.ENTORNO! || (Entorno.CERTIFICACION as Entorno);
