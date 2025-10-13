import RDP02_DB_INSTANCES from "../../../connectors/postgres";
import { NivelEducativo } from "../../../../../interfaces/shared/NivelEducativo";
import { T_Ajustes_Generales_Sistema } from "@prisma/client";

interface ConfiguracionLimiteArchivos {
  faltasConsecutivasMaximas: number;
  tardanzasConsecutivasMaximas: number;
  limiteArchivos: number; // El mayor de los dos
}

/**
 * Obtiene las configuraciones de faltas y tardanzas consecutivas máximas
 * y determina el límite de archivos a mantener
 */
export async function obtenerLimiteTardanzasFaltasEscolaresConsecutivas(
  nivel: NivelEducativo
): Promise<ConfiguracionLimiteArchivos> {
  try {
    const sufijo =
      nivel === NivelEducativo.PRIMARIA ? "PRIMARIA" : "SECUNDARIA";

    const sql = `
      SELECT "Nombre", "Valor"
      FROM "T_Ajustes_Generales_Sistema"
      WHERE "Nombre" IN (
        'FALTAS_CONSECUTIVAS_MAXIMAS_ALERTA_ESTUDIANTES_${sufijo}',
        'TARDANZAS_CONSECUTIVAS_MAXIMAS_ALERTA_ESTUDIANTES_${sufijo}'
      )
    `;

    const result = await RDP02_DB_INSTANCES.query(sql);

    let faltasConsecutivasMaximas = 3; // Valor por defecto
    let tardanzasConsecutivasMaximas = 3; // Valor por defecto

    result.rows.forEach((row: T_Ajustes_Generales_Sistema) => {
      const valor = parseInt(row.Valor, 10);
      if (row.Nombre.includes("FALTAS")) {
        faltasConsecutivasMaximas = valor;
      } else if (row.Nombre.includes("TARDANZAS")) {
        tardanzasConsecutivasMaximas = valor;
      }
    });

    // El límite de archivos es el MAYOR de los dos
    const limiteArchivos = Math.max(
      faltasConsecutivasMaximas,
      tardanzasConsecutivasMaximas
    );

    console.log(`📊 Configuración límite de archivos para ${sufijo}:`);
    console.log(
      `   - Faltas consecutivas máximas: ${faltasConsecutivasMaximas}`
    );
    console.log(
      `   - Tardanzas consecutivas máximas: ${tardanzasConsecutivasMaximas}`
    );
    console.log(`   - Límite de archivos a mantener: ${limiteArchivos}`);

    return {
      faltasConsecutivasMaximas,
      tardanzasConsecutivasMaximas,
      limiteArchivos,
    };
  } catch (error) {
    console.error(
      "Error obteniendo configuración de límite de archivos:",
      error,
      "Se usaran valores por defecto."
    );
    // Retornar valores por defecto en caso de error
    return {
      faltasConsecutivasMaximas: 3,
      tardanzasConsecutivasMaximas: 3,
      limiteArchivos: 3,
    };
  }
}
