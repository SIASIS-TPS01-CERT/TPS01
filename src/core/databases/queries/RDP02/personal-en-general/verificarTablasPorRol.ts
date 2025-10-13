import RDP02_DB_INSTANCES from "../../../connectors/postgres";

/**
 * Verifica qué tablas de control de asistencia existen realmente en la base de datos
 * 🆕 ACTUALIZADA para incluir tablas de directivos
 * @returns Map con nombres de tabla esperados como clave y nombres reales como valor
 */
export async function verificarTablasPorRol(): Promise<Map<string, string>> {
  try {
    // Lista de todas las tablas que el sistema espera encontrar
    const tablasEsperadas = [
      // 🆕 NUEVAS TABLAS PARA DIRECTIVOS
      "T_Control_Entrada_Mensual_Directivos",
      "T_Control_Salida_Mensual_Directivos",

      // Tablas existentes
      "T_Control_Entrada_Mensual_Auxiliar",
      "T_Control_Salida_Mensual_Auxiliar",
      "T_Control_Entrada_Mensual_Profesores_Primaria",
      "T_Control_Salida_Mensual_Profesores_Primaria",
      "T_Control_Entrada_Mensual_Profesores_Secundaria",
      "T_Control_Salida_Mensual_Profesores_Secundaria",
      "T_Control_Entrada_Mensual_Personal_Administrativo",
      "T_Control_Salida_Mensual_Personal_Administrativo",
    ];

    console.log(
      `🔍 Verificando existencia de ${tablasEsperadas.length} tablas...`
    );

    // Construir consulta SQL para verificar existencia de tablas
    const placeholders = tablasEsperadas
      .map((_, index) => `$${index + 1}`)
      .join(", ");
    const sql = `
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public'
        AND table_name IN (${placeholders})
    `;

    const result = await RDP02_DB_INSTANCES.query(sql, tablasEsperadas, false);

    // Crear mapa de tablas que existen
    const tablasExistentes = new Map<string, string>();
    const tablasEncontradas = result.rows.map((row: any) => row.table_name);

    // Mapear tablas esperadas con las encontradas
    for (const tabla of tablasEsperadas) {
      if (tablasEncontradas.includes(tabla)) {
        tablasExistentes.set(tabla, tabla);
      }
    }

    console.log(
      `✅ Tablas encontradas: ${tablasExistentes.size}/${tablasEsperadas.length}`
    );

    // Reportar tablas faltantes
    const tablasFaltantes = tablasEsperadas.filter(
      (tabla) => !tablasExistentes.has(tabla)
    );
    if (tablasFaltantes.length > 0) {
      console.warn(`⚠️  Tablas faltantes (${tablasFaltantes.length}):`);
      tablasFaltantes.forEach((tabla) => {
        console.warn(`   - ${tabla}`);
      });
    }

    // 🆕 Reportar específicamente sobre tablas de directivos
    const tablasDirectivos = [
      "T_Control_Entrada_Mensual_Directivos",
      "T_Control_Salida_Mensual_Directivos",
    ];

    const tablasDirectivosExistentes = tablasDirectivos.filter((tabla) =>
      tablasExistentes.has(tabla)
    );
    console.log(
      `🏢 Tablas de directivos encontradas: ${tablasDirectivosExistentes.length}/${tablasDirectivos.length}`
    );

    if (tablasDirectivosExistentes.length === tablasDirectivos.length) {
      console.log(
        "✅ Soporte completo para asistencia de directivos disponible"
      );
    } else {
      console.warn(
        "⚠️  Soporte parcial para directivos - algunas tablas faltan:"
      );
      tablasDirectivos
        .filter((tabla) => !tablasExistentes.has(tabla))
        .forEach((tabla) => {
          console.warn(`   - ${tabla}`);
        });
    }

    return tablasExistentes;
  } catch (error) {
    console.error("❌ Error al verificar tablas por rol:", error);
    // Retornar mapa vacío en caso de error
    return new Map<string, string>();
  }
}
