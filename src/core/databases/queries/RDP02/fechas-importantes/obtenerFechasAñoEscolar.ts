import RDP02_DB_INSTANCES from "../../../connectors/postgres";

export async function obtenerFechasAñoEscolar() {
  const sql = `
    SELECT "Nombre", "Valor"
    FROM "T_Fechas_Importantes"
    WHERE "Nombre" IN (
      'Fecha_Inicio_Año_Escolar',
      'Fecha_Fin_Año_Escolar'
    )
  `;

  const result = await RDP02_DB_INSTANCES.query(sql);

  // Convertir a un objeto para fácil acceso
  const fechas = result.rows.reduce((acc: any, row: any) => {
    acc[row.Nombre.replace('Fecha_', '')] = new Date(row.Valor);
    return acc;
  }, {});

  // Verificar que se obtuvieron ambas fechas, de lo contrario usar valores predeterminados
  if (!fechas.Inicio_Año_Escolar) {
    fechas.Inicio_Año_Escolar = new Date(new Date().getFullYear(), 2, 1); // 1 de marzo del año actual
  }
  
  if (!fechas.Fin_Año_Escolar) {
    fechas.Fin_Año_Escolar = new Date(new Date().getFullYear(), 11, 31); // 31 de diciembre del año actual
  }

  return fechas;
}