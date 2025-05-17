import RDP02_DB_INSTANCES from '../../../connectors/postgres';

export async function verificarTablasPorRol(): Promise<Map<string, string>> {
  // Lista de todas las posibles tablas de control de asistencia
  const tablasNecesarias = [
    "T_Control_Entrada_Mensual_Auxiliar",
    "T_Control_Salida_Mensual_Auxiliar",
    "T_Control_Entrada_Mensual_Profesores_Primaria",
    "T_Control_Salida_Mensual_Profesores_Primaria",
    "T_Control_Entrada_Mensual_Profesores_Secundaria",
    "T_Control_Salida_Mensual_Profesores_Secundaria",
    "T_Control_Entrada_Mensual_Personal_Administrativo",
    "T_Control_Salida_Mensual_Personal_Administrativo",
  ];

  // Convertir a formato para PostgreSQL
  const tablasFormateadas = tablasNecesarias.map((t) => {
    return t
      .split("_")
      .map((word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
      .join("_");
  });

  // Convertir a minúsculas para comparación
  const tablasMinusculas = tablasFormateadas.map((t) => t.toLowerCase());

  // Verificar qué tablas existen
  const sql = `
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    AND (table_name = ANY($1) OR LOWER(table_name) = ANY($2))
  `;

  try {
    const result = await RDP02_DB_INSTANCES.query(sql, [tablasFormateadas, tablasMinusculas]);

    // Crear un mapeo entre los nombres originales y los nombres reales
    const tablasExistentes = new Map<string, string>();

    console.log(
      "Tablas encontradas en la base de datos:",
      result.rows.map((r: any) => r.table_name)
    );

    // Para cada nombre original, buscar su correspondiente en el resultado
    tablasNecesarias.forEach((nombreOriginal, index) => {
      const tablaFormateada = tablasFormateadas[index];
      const tablaMinuscula = tablasMinusculas[index];

      const encontrada = result.rows.find(
        (row: any) => row.table_name.toLowerCase() === tablaMinuscula
      );

      if (encontrada) {
        tablasExistentes.set(nombreOriginal, encontrada.table_name);
      }
    });

    console.log("Mapeo de tablas:", Object.fromEntries(tablasExistentes));

    return tablasExistentes;
  } catch (error) {
    console.error("Error al verificar tablas de control de asistencia:", error);
    return new Map();
  }
}
