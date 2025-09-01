import { ENTORNO } from "./ENTORNO";
import { Entorno } from "../interfaces/shared/Entornos";

// Configuración para cada blob
export const RDP04_INSTANCES_DISTRIBUTION =
  ENTORNO === Entorno.PRODUCCION
    ? //CASO PRODUCCION
      [
        {
          nombre: "INS1",
          tokenEnv: "RDP04_VERCEL_BLOB_INS1_READ_WRITE_TOKEN",
        },
        {
          nombre: "INS2",
          tokenEnv: "RDP04_VERCEL_BLOB_INS2_READ_WRITE_TOKEN",
        },
        {
          nombre: "INS3",
          tokenEnv: "RDP04_VERCEL_BLOB_INS3_READ_WRITE_TOKEN",
        },
        {
          nombre: "INS4",
          tokenEnv: "RDP04_VERCEL_BLOB_INS4_READ_WRITE_TOKEN",
        },
        {
          nombre: "INS5",
          tokenEnv: "RDP04_VERCEL_BLOB_INS5_READ_WRITE_TOKEN",
        },
      ]
    : ENTORNO === Entorno.CERTIFICACION
    ? //CASO CERTIFICACION
      [
        {
          nombre: "INS1",
          tokenEnv: "RDP04_VERCEL_BLOB_INS1_READ_WRITE_TOKEN",
        },
        {
          nombre: "INS2",
          tokenEnv: "RDP04_VERCEL_BLOB_INS2_READ_WRITE_TOKEN",
        },
        {
          nombre: "INS3",
          tokenEnv: "RDP04_VERCEL_BLOB_INS3_READ_WRITE_TOKEN",
        },
        {
          nombre: "INS4",
          tokenEnv: "RDP04_VERCEL_BLOB_INS4_READ_WRITE_TOKEN",
        },
        {
          nombre: "INS5",
          tokenEnv: "RDP04_VERCEL_BLOB_INS5_READ_WRITE_TOKEN",
        },
      ]
    : //CASO DESARROLLO O LOCAL
      [
        {
          nombre: "INS1",
          tokenEnv: "RDP04_VERCEL_BLOB_INS1_READ_WRITE_TOKEN",
        },
        {
          nombre: "INS2",
          tokenEnv: "RDP04_VERCEL_BLOB_INS2_READ_WRITE_TOKEN",
        },
      ];
