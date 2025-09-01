import { RolesSistema } from "../../../../../interfaces/shared/RolesSistema";
import RDP02_DB_INSTANCES from '../../../connectors/postgres';

interface PersonalInactivo {
  id: string;
  rol: RolesSistema;
  tablaMensualEntrada: string;
  tablaMensualSalida: string;
  campoId: string;
  campoIdUsuario: string;
}

export async function obtenerPersonalInactivoParaRegistroAutomatico(): Promise<
  PersonalInactivo[]
> {
  // Crear una lista con todos los registros de personal inactivo
  const personalInactivo: PersonalInactivo[] = [];

  // Verificar qué tablas existen antes de ejecutar las consultas
  const tablasExistentes = await verificarTablasExistentes([
    "T_Auxiliares",
    "T_Profesores_Primaria",
    "T_Profesores_Secundaria",
    "T_Personal_Administrativo",
  ]);

  // 1. Auxiliares inactivos (si la tabla existe)
  if (tablasExistentes.includes("T_Auxiliares")) {
    try {
      const sqlAuxiliares = `
        SELECT "Id_Auxiliar"
        FROM "T_Auxiliares"
        WHERE "Estado" = false
      `;

      const auxiliaresInactivos = await RDP02_DB_INSTANCES.query(sqlAuxiliares);
      auxiliaresInactivos.rows.forEach((row: any) => {
        personalInactivo.push({
          id: row.Id_Auxiliar,
          rol: RolesSistema.Auxiliar,
          tablaMensualEntrada: "t_Control_Entrada_Mensual_Auxiliar",
          tablaMensualSalida: "t_Control_Salida_Mensual_Auxiliar",
          campoId: "Id_C_E_M_P_Auxiliar",
          campoIdUsuario: "Id_Auxiliar",
        });
      });
    } catch (error) {
      console.warn("Error al obtener auxiliares inactivos:", error);
    }
  }

  // 2. Profesores de primaria inactivos (si la tabla existe)
  if (tablasExistentes.includes("T_Profesores_Primaria")) {
    try {
      const sqlProfesoresPrimaria = `
        SELECT "Id_Profesor_Primaria"
        FROM "T_Profesores_Primaria"
        WHERE "Estado" = false
      `;

      const profesoresPrimariaInactivos = await RDP02_DB_INSTANCES.query(sqlProfesoresPrimaria);
      profesoresPrimariaInactivos.rows.forEach((row: any) => {
        personalInactivo.push({
          id: row.Id_Profesor_Primaria,
          rol: RolesSistema.ProfesorPrimaria,
          tablaMensualEntrada: "t_Control_Entrada_Mensual_Profesores_Primaria",
          tablaMensualSalida: "t_Control_Salida_Mensual_Profesores_Primaria",
          campoId: "Id_C_E_M_P_Profesores_Primaria",
          campoIdUsuario: "Id_Profesor_Primaria",
        });
      });
    } catch (error) {
      console.warn("Error al obtener profesores de primaria inactivos:", error);
    }
  }

  // 3. Profesores de secundaria inactivos (si la tabla existe)
  if (tablasExistentes.includes("T_Profesores_Secundaria")) {
    try {
      const sqlProfesoresSecundaria = `
        SELECT "Id_Profesor_Secundaria"
        FROM "T_Profesores_Secundaria"
        WHERE "Estado" = false
      `;

      const profesoresSecundariaInactivos = await RDP02_DB_INSTANCES.query(
        sqlProfesoresSecundaria
      );
      profesoresSecundariaInactivos.rows.forEach((row: any) => {
        personalInactivo.push({
          id: row.Id_Profesor_Secundaria,
          rol: RolesSistema.ProfesorSecundaria,
          tablaMensualEntrada:
            "t_Control_Entrada_Mensual_Profesores_Secundaria",
          tablaMensualSalida: "t_Control_Salida_Mensual_Profesores_Secundaria",
          campoId: "Id_C_E_M_P_Profesores_Secundaria",
          campoIdUsuario: "Id_Profesor_Secundaria",
        });
      });
    } catch (error) {
      console.warn(
        "Error al obtener profesores de secundaria inactivos:",
        error
      );
    }
  }

  // 4. Personal administrativo inactivo (si la tabla existe)
  if (tablasExistentes.includes("T_Personal_Administrativo")) {
    try {
      const sqlPersonalAdministrativo = `
        SELECT "Id_Personal_Administrativo"
        FROM "T_Personal_Administrativo"
        WHERE "Estado" = false
      `;

      const personalAdministrativoInactivo = await RDP02_DB_INSTANCES.query(
        sqlPersonalAdministrativo
      );
      personalAdministrativoInactivo.rows.forEach((row: any) => {
        personalInactivo.push({
          id: row.Id_Personal_Administrativo,
          rol: RolesSistema.PersonalAdministrativo,
          tablaMensualEntrada:
            "t_Control_Entrada_Mensual_Personal_Administrativo",
          tablaMensualSalida:
            "t_Control_Salida_Mensual_Personal_Administrativo",
          campoId: "Id_C_E_M_P_Administrativo",
          campoIdUsuario: "Id_Personal_Administrativo",
        });
      });
    } catch (error) {
      console.warn("Error al obtener personal administrativo inactivo:", error);
    }
  }

  return personalInactivo;
}

// Función para verificar qué tablas existen en la base de datos
async function verificarTablasExistentes(tablas: string[]): Promise<string[]> {
  const tablasExistentes: string[] = [];

  // Convertimos los nombres a la forma correcta según tus capturas:
  // - Con "T_" al inicio si aún no lo tienen
  // - Con cada palabra capitalizada correctamente
  const tablasFormateadas = tablas.map((t) => {
    // Si no comienza con T_ o t_, agregarlo
    const withPrefix = t.startsWith("T_") || t.startsWith("t_") ? t : `T_${t}`;

    // Convertir a formato PostgreSQL con primera letra de cada palabra en mayúscula
    return withPrefix
      .split("_")
      .map((word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
      .join("_");
  });

  const sql = `
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    AND (table_name = ANY($1) OR LOWER(table_name) = ANY($2))
  `;

  try {
    // Verificamos con los nombres formateados y también con versiones en minúsculas
    const tablasMinusculas = tablasFormateadas.map((t) => t.toLowerCase());
    const result = await RDP02_DB_INSTANCES.query(sql, [tablasFormateadas, tablasMinusculas]);

    console.log(
      "Tablas encontradas en la base de datos:",
      result.rows.map((r: any) => r.table_name)
    );

    result.rows.forEach((row: any) => {
      // Guardamos el nombre exacto como aparece en la base de datos
      tablasExistentes.push(row.table_name);
    });
  } catch (error) {
    console.warn("Error al verificar tablas existentes:", error);
  }

  return tablasExistentes;
}
