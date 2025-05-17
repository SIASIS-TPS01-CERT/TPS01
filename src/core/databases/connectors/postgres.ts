// src/core/database/connectors/postgres.ts
import { Pool } from "pg";
import dotenv from "dotenv";
import {
  PG_CONNECTION_TIMEOUT,
  PG_IDLE_TIMEOUT,
  PG_MAX_CONNECTIONS,
} from "../../../constants/NEON_POSTGRES_CONFIG";
import { RDP02 } from "../../../interfaces/shared/RDP02Instancias";
import { RolesSistema } from "../../../interfaces/shared/RolesSistema";
import {
  AUXILIAR_INSTANCES,
  DIRECTIVO_INSTANCES,
  PERSONAL_ADMIN_INSTANCES,
  PROFESOR_PRIMARIA_INSTANCES,
  PROFESOR_SECUNDARIA_INSTANCES,
  RDP02_INSTANCES_DATABASE_URL_MAP,
  TUTOR_INSTANCES,
} from "../../../constants/RDP02_INSTANCES_DISTRIBUTION";

dotenv.config();

// Agrupar roles por afinidad de acceso
type RolesGroup = {
  name: string;
  roles: RolesSistema[];
  instances: RDP02[];
};

const rolesGroups: RolesGroup[] = [
  {
    name: "Administrativos",
    roles: [
      RolesSistema.Directivo,
      RolesSistema.Auxiliar,
      RolesSistema.PersonalAdministrativo,
      RolesSistema.Responsable,
    ],
    instances: [
      ...new Set([
        ...DIRECTIVO_INSTANCES,
        ...AUXILIAR_INSTANCES,
        ...PERSONAL_ADMIN_INSTANCES,
      ]),
    ],
  },
  {
    name: "Secundaria",
    roles: [RolesSistema.ProfesorSecundaria, RolesSistema.Tutor],
    instances: [
      ...new Set([...PROFESOR_SECUNDARIA_INSTANCES, ...TUTOR_INSTANCES]),
    ],
  },
  {
    name: "Primaria",
    roles: [RolesSistema.ProfesorPrimaria],
    instances: PROFESOR_PRIMARIA_INSTANCES,
  },
];

// Estructura para almacenar las instancias de pools de PostgreSQL
type PostgresInstances = {
  [key: string]: Pool[];
};

// Inicialización de las instancias de PostgreSQL basadas en la configuración
const postgresInstances: PostgresInstances = {};

// Inicializar los pools de conexión para cada grupo y sus instancias
for (const group of rolesGroups) {
  postgresInstances[group.name] = [];

  for (const instanceId of group.instances) {
    const connectionString = RDP02_INSTANCES_DATABASE_URL_MAP.get(instanceId);

    if (connectionString) {
      postgresInstances[group.name].push(
        new Pool({
          connectionString,
          max: parseInt(PG_MAX_CONNECTIONS || "3", 10),
          idleTimeoutMillis: parseInt(PG_IDLE_TIMEOUT || "10000", 10),
          connectionTimeoutMillis: parseInt(
            PG_CONNECTION_TIMEOUT || "5000",
            10
          ),
          ssl: true,
        })
      );
    } else {
      console.warn(
        `No se encontró URL de conexión para la instancia ${instanceId}`
      );
    }
  }
}

// Cache simple para reducir consultas repetitivas
// Ahora el cache está separado por grupo
const queryCaches: { [key: string]: Map<string, any> } = {};

// Inicializar los caches para cada grupo
for (const group of rolesGroups) {
  queryCaches[group.name] = new Map();
}

const CACHE_TTL = 60000; // 1 minuto en milisegundos

// Función para determinar el grupo de un rol
function getRoleGroup(role?: RolesSistema): string | null {
  if (!role) return null; // Si no se especifica rol, retornar null

  for (const group of rolesGroups) {
    if (group.roles.includes(role)) {
      return group.name;
    }
  }

  return null; // Si no se encuentra el rol, retornar null
}

// Función para obtener una instancia aleatoria de todos los pools disponibles
function getRandomPoolFromAll(): Pool {
  const allPools: Pool[] = [];

  // Recopilar todos los pools de todos los grupos
  for (const groupName in postgresInstances) {
    allPools.push(...postgresInstances[groupName]);
  }

  if (allPools.length === 0) {
    throw new Error("No hay instancias de PostgreSQL disponibles");
  }

  const randomIndex = Math.floor(Math.random() * allPools.length);
  return allPools[randomIndex];
}

// Función para obtener una instancia aleatoria de PostgreSQL por grupo
function getRandomPool(group?: string): Pool {
  // Si no se especifica grupo, obtener una instancia aleatoria de todas
  if (!group) {
    return getRandomPoolFromAll();
  }

  const instances = postgresInstances[group];
  if (!instances || instances.length === 0) {
    throw new Error(`No hay instancias disponibles para el grupo: ${group}`);
  }

  const randomIndex = Math.floor(Math.random() * instances.length);
  return instances[randomIndex];
}

// Función para agregar una nueva instancia de PostgreSQL
export function addPostgresInstance(
  group: string,
  connectionString: string
): void {
  if (!postgresInstances[group]) {
    postgresInstances[group] = [];
  }

  postgresInstances[group].push(
    new Pool({
      connectionString,
      max: parseInt(PG_MAX_CONNECTIONS || "3", 10),
      idleTimeoutMillis: parseInt(PG_IDLE_TIMEOUT || "10000", 10),
      connectionTimeoutMillis: parseInt(PG_CONNECTION_TIMEOUT || "5000", 10),
      ssl: true,
    })
  );
}

// Función para ejecutar en todas las instancias de todos los grupos
async function executeOnAllInstances(
  text: string,
  params?: any[],
  maxRetries: number = 3
): Promise<any> {
  const allResults = [];

  // Ejecutar en cada grupo
  for (const groupName in postgresInstances) {
    const result = await executeOnAllInstancesInGroup(
      groupName,
      text,
      params,
      maxRetries
    );
    if (result) {
      allResults.push(result);
    }
  }

  // Retornar el primer resultado válido (para compatibilidad)
  return allResults.length > 0 ? allResults[0] : null;
}

// Función para ejecutar consultas con caché opcional y reintentos
export async function query(
  text: string,
  params?: any[],
  options: {
    useCache?: boolean;
    maxRetries?: number;
    role?: RolesSistema;
    executeOnAllInstances?: boolean;
  } = {}
) {
  const {
    useCache = false,
    maxRetries = 3,
    role,
    executeOnAllInstances: executeOnAllInstancesOption = false,
  } = options;

  // Si se pide ejecutar en todas las instancias o no se especifica un rol
  if (executeOnAllInstancesOption) {
    if (!role) {
      // Si no hay rol, ejecutar en todas las instancias de todos los grupos
      return executeOnAllInstances(text, params, maxRetries);
    } else {
      // Si hay rol, ejecutar en todas las instancias del grupo correspondiente
      const group = getRoleGroup(role);
      if (group) {
        return executeOnAllInstancesInGroup(group, text, params, maxRetries);
      } else {
        // Si el rol no corresponde a ningún grupo, ejecutar en todas las instancias
        return executeOnAllInstances(text, params, maxRetries);
      }
    }
  }

  // Para consultas normales (no executeOnAllInstances)
  const group = role ? getRoleGroup(role) : null;
  const queryCache = group ? queryCaches[group] : new Map(); // Cache temporal si no hay grupo

  let retries = 0;
  let lastError;

  while (retries < maxRetries) {
    try {
      // Intentar usar caché si está habilitado y hay un grupo específico
      if (useCache && group) {
        const cacheKey = `${text}-${JSON.stringify(params || [])}`;
        const cachedItem = queryCache.get(cacheKey);

        if (cachedItem && Date.now() - cachedItem.timestamp < CACHE_TTL) {
          console.log(`Cache hit (${group}):`, cacheKey);
          return cachedItem.result;
        }
      }

      // Si no hay caché o está desactualizado, ejecutar la consulta
      const start = Date.now();
      const pool = getRandomPool(group || undefined);
      const client = await pool.connect();

      try {
        const res = await client.query(text, params);
        const duration = Date.now() - start;

        console.log(`Query ejecutada (${group || "cualquier grupo"})`, {
          text: text.substring(0, 80) + (text.length > 80 ? "..." : ""),
          duration,
          filas: res.rowCount,
        });

        // Guardar en caché si está habilitado y hay un grupo específico
        if (useCache && group) {
          const cacheKey = `${text}-${JSON.stringify(params || [])}`;
          queryCache.set(cacheKey, {
            timestamp: Date.now(),
            result: res,
          });
        }

        return res;
      } finally {
        // Siempre liberar el cliente al terminar
        client.release();
      }
    } catch (error) {
      lastError = error;
      retries++;
      console.error(
        `Error en intento ${retries}/${maxRetries} (${
          group || "cualquier grupo"
        }):`,
        error
      );

      if (retries < maxRetries) {
        // Espera exponencial entre reintentos (1s, 2s, 4s, etc.)
        const waitTime = 1000 * Math.pow(2, retries - 1);
        console.log(`Reintentando en ${waitTime / 1000} segundos...`);
        await new Promise((resolve) => setTimeout(resolve, waitTime));
      }
    }
  }

  console.error(
    `Error en consulta SQL después de todos los reintentos (${
      group || "cualquier grupo"
    }):`,
    lastError
  );
  throw lastError;
}

// Función para ejecutar una consulta en todas las instancias de un grupo
async function executeOnAllInstancesInGroup(
  group: string,
  text: string,
  params?: any[],
  maxRetries: number = 3
): Promise<any> {
  const instances = postgresInstances[group];
  if (!instances || instances.length === 0) {
    console.warn(`No hay instancias disponibles para el grupo: ${group}`);
    return null;
  }

  const results = [];

  for (const pool of instances) {
    let retries = 0;
    let lastError;
    let success = false;

    while (retries < maxRetries && !success) {
      try {
        const client = await pool.connect();
        try {
          const res = await client.query(text, params);
          results.push(res);
          success = true;
        } finally {
          client.release();
        }
      } catch (error) {
        lastError = error;
        retries++;

        if (retries < maxRetries) {
          const waitTime = 1000 * Math.pow(2, retries - 1);
          await new Promise((resolve) => setTimeout(resolve, waitTime));
        }
      }
    }

    if (!success) {
      console.error(
        `No se pudo ejecutar en todas las instancias del grupo ${group}:`,
        lastError
      );
      throw lastError;
    }
  }

  // Devolver el resultado de la primera instancia (principalmente para compatibilidad)
  return results.length > 0 ? results[0] : null;
}

// Función para cerrar todos los pools (útil al finalizar el script)
export async function closeAllPools() {
  const closePromises = [];

  for (const group in postgresInstances) {
    for (const pool of postgresInstances[group]) {
      closePromises.push(pool.end());
    }
  }

  await Promise.all(closePromises);
  console.log("Todos los pools de conexión han sido cerrados");
}

// Limpiar caché periódicamente para todos los grupos
setInterval(() => {
  const now = Date.now();

  for (const group in queryCaches) {
    const queryCache = queryCaches[group];
    for (const [key, value] of queryCache.entries()) {
      if (now - value.timestamp > CACHE_TTL) {
        queryCache.delete(key);
      }
    }
  }
}, CACHE_TTL);

// Mantener compatibilidad con la versión anterior
export async function closePool() {
  await closeAllPools();
}

// Función para detectar si es una operación de escritura
function isWriteOperation(text: string): boolean {
  const upperText = text.trim().toUpperCase();
  return (
    upperText.startsWith("INSERT") ||
    upperText.startsWith("UPDATE") ||
    upperText.startsWith("DELETE") ||
    upperText.includes("CREATE TABLE") ||
    upperText.includes("ALTER TABLE") ||
    upperText.includes("DROP TABLE")
  );
}

// Para compatibilidad con código existente que espera la función query original
const defaultQuery = async (
  text: string,
  params?: any[],
  useCache: boolean = false
) => {
  // Detectar si es una operación de escritura
  if (isWriteOperation(text)) {
    // Si es escritura, ejecutar en TODAS las instancias de TODOS los grupos
    return executeOnAllInstances(text, params);
  } else {
    // Para operaciones de lectura, ejecutar en una instancia aleatoria
    return query(text, params, { useCache });
  }
};

const RDP02_DB_INSTANCES = {
  query: defaultQuery,
  closePool,
};

export default RDP02_DB_INSTANCES;
