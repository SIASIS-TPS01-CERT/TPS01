import { MongoClient, Db, MongoClientOptions } from "mongodb";
import dotenv from "dotenv";
import {
  MONGO_CONNECTION_TIMEOUT,
  MONGO_SERVER_SELECTION_TIMEOUT,
  MONGO_MAX_POOL_SIZE,
  MONGO_MIN_POOL_SIZE,
} from "../../../constants/MONGODB_ATLAS_MONGODB_CONFIG";
import { RolesSistema } from "../../../interfaces/shared/RolesSistema";
import { RDP03 } from "../../../interfaces/shared/RDP03Instancias";
import {
  AUXILIAR_INSTANCES,
  DIRECTIVO_INSTANCES,
  PERSONAL_ADMIN_INSTANCES,
  PROFESOR_PRIMARIA_INSTANCES,
  PROFESOR_SECUNDARIA_INSTANCES,
  RDP03_INSTANCES_DATABASE_URL_MAP,
  TUTOR_INSTANCES,
  RESPONSABLE_INSTANCES,
} from "../../../constants/RDP03_INSTANCES_DISTRIBUTION";
import { MongoOperation } from "../../../interfaces/shared/RDP03/MongoOperation";

dotenv.config();

// Agrupar roles por afinidad de acceso (similar al sistema PostgreSQL)
type RolesGroup = {
  name: string;
  roles: RolesSistema[];
  instances: RDP03[];
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
        ...RESPONSABLE_INSTANCES,
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

// Estructura para almacenar las instancias de clientes MongoDB
type MongoDBInstances = {
  [key: string]: MongoClient[];
};

// Inicialización de las instancias de MongoDB basadas en la configuración
const mongoDBInstances: MongoDBInstances = {};
const dbInstances: { [key: string]: Db[] } = {};

// Opciones de configuración para MongoDB
const mongoOptions: MongoClientOptions = {
  maxPoolSize: parseInt(MONGO_MAX_POOL_SIZE || "10", 10),
  minPoolSize: parseInt(MONGO_MIN_POOL_SIZE || "2", 10),
  maxIdleTimeMS: 30000,
  serverSelectionTimeoutMS: parseInt(
    MONGO_SERVER_SELECTION_TIMEOUT || "5000",
    10
  ),
  connectTimeoutMS: parseInt(MONGO_CONNECTION_TIMEOUT || "10000", 10),
  heartbeatFrequencyMS: 10000,
  retryWrites: true,
  retryReads: true,
};

// Inicializar los clientes MongoDB para cada grupo y sus instancias
async function initializeMongoDBConnections() {
  for (const group of rolesGroups) {
    mongoDBInstances[group.name] = [];
    dbInstances[group.name] = [];

    for (const instanceId of group.instances) {
      const connectionString = RDP03_INSTANCES_DATABASE_URL_MAP.get(instanceId);

      if (connectionString) {
        try {
          const client = new MongoClient(connectionString, mongoOptions);
          await client.connect();

          // Agregar manejadores de eventos
          client.on("error", (err: any) => {
            console.error(`Error en cliente MongoDB ${instanceId}:`, err);
          });

          client.on("close", () => {
            console.log(`Conexión MongoDB ${instanceId} cerrada`);
          });

          mongoDBInstances[group.name].push(client);
          dbInstances[group.name].push(client.db("siasis_asuncion_8"));

          console.log(
            `Cliente MongoDB inicializado para instancia ${instanceId}`
          );
        } catch (error) {
          console.error(
            `Error al conectar con la instancia MongoDB ${instanceId}:`,
            error
          );
        }
      } else {
        console.warn(
          `No se encontró URL de conexión para la instancia ${instanceId}`
        );
      }
    }
  }
}

// Inicializar conexiones al importar el módulo
initializeMongoDBConnections().catch(console.error);

// Cache simple para reducir consultas repetitivas
const queryCaches: { [key: string]: Map<string, any> } = {};

// Inicializar los caches para cada grupo
for (const group of rolesGroups) {
  queryCaches[group.name] = new Map();
}

const CACHE_TTL = 60000; // 1 minuto en milisegundos

// Función para determinar el grupo de un rol
function getRoleGroup(role?: RolesSistema): string | null {
  if (!role) return null;

  for (const group of rolesGroups) {
    if (group.roles.includes(role)) {
      return group.name;
    }
  }

  return null;
}

// Función para obtener una instancia aleatoria de todos los clientes disponibles
function getRandomClientFromAll(): { client: MongoClient; db: Db } {
  const allClients: MongoClient[] = [];
  const allDbs: Db[] = [];

  // Recopilar todos los clientes de todos los grupos
  for (const groupName in mongoDBInstances) {
    allClients.push(...mongoDBInstances[groupName]);
    allDbs.push(...dbInstances[groupName]);
  }

  if (allClients.length === 0) {
    throw new Error("No hay instancias de MongoDB disponibles");
  }

  const randomIndex = Math.floor(Math.random() * allClients.length);
  return { client: allClients[randomIndex], db: allDbs[randomIndex] };
}

// Función para obtener una instancia aleatoria de MongoDB por grupo
function getRandomClient(group?: string): { client: MongoClient; db: Db } {
  // Si no se especifica grupo, obtener una instancia aleatoria de todas
  if (!group) {
    return getRandomClientFromAll();
  }

  const clients = mongoDBInstances[group];
  const dbs = dbInstances[group];

  if (!clients || clients.length === 0) {
    throw new Error(`No hay instancias disponibles para el grupo: ${group}`);
  }

  const randomIndex = Math.floor(Math.random() * clients.length);
  return { client: clients[randomIndex], db: dbs[randomIndex] };
}

// Función para ejecutar en todas las instancias de todos los grupos
async function executeOnAllInstances(
  operation: MongoOperation,
  maxRetries: number = 3
): Promise<any> {
  const allResults = [];

  // Ejecutar en cada grupo
  for (const groupName in mongoDBInstances) {
    const result = await executeOnAllInstancesInGroup(
      groupName,
      operation,
      maxRetries
    );
    if (result) {
      allResults.push(result);
    }
  }

  // Retornar el primer resultado válido (para compatibilidad)
  return allResults.length > 0 ? allResults[0] : null;
}

// Función para ejecutar una operación en todas las instancias de un grupo
async function executeOnAllInstancesInGroup(
  group: string,
  operation: MongoOperation,
  maxRetries: number = 3
): Promise<any> {
  const clients = mongoDBInstances[group];
  const dbs = dbInstances[group];

  if (!clients || clients.length === 0) {
    console.warn(`No hay instancias disponibles para el grupo: ${group}`);
    return null;
  }

  const results = [];

  for (let i = 0; i < clients.length; i++) {
    const db = dbs[i];
    let retries = 0;
    let lastError;
    let success = false;

    while (retries < maxRetries && !success) {
      try {
        const result = await executeMongoOperationOnDB(db, operation);
        results.push(result);
        success = true;
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

// Función auxiliar para ejecutar operación MongoDB en una base de datos específica
async function executeMongoOperationOnDB(
  db: Db,
  operation: MongoOperation
): Promise<any> {
  const collection = db.collection(operation.collection);

  switch (operation.operation) {
    case "find":
      return await collection
        .find(operation.filter || {}, operation.options)
        .toArray();

    case "findOne":
      return await collection.findOne(
        operation.filter || {},
        operation.options
      );

    case "insertOne":
      return await collection.insertOne(operation.data, operation.options);

    case "insertMany":
      return await collection.insertMany(operation.data, operation.options);

    case "updateOne":
      return await collection.updateOne(
        operation.filter || {},
        operation.data,
        operation.options
      );

    case "updateMany":
      return await collection.updateMany(
        operation.filter || {},
        operation.data,
        operation.options
      );

    case "deleteOne":
      return await collection.deleteOne(
        operation.filter || {},
        operation.options
      );

    case "deleteMany":
      return await collection.deleteMany(
        operation.filter || {},
        operation.options
      );

    case "replaceOne":
      return await collection.replaceOne(
        operation.filter || {},
        operation.data,
        operation.options
      );

    case "aggregate":
      return await collection
        .aggregate(operation.pipeline || [], operation.options)
        .toArray();

    case "countDocuments":
      return await collection.countDocuments(
        operation.filter || {},
        operation.options
      );

    default:
      throw new Error(`Operación no soportada: ${operation.operation}`);
  }
}

// Función principal para ejecutar operaciones MongoDB con caché opcional y reintentos
export async function executeMongoDBOperation(
  operation: MongoOperation,
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
      return executeOnAllInstances(operation, maxRetries);
    } else {
      // Si hay rol, ejecutar en todas las instancias del grupo correspondiente
      const group = getRoleGroup(role);
      if (group) {
        return executeOnAllInstancesInGroup(group, operation, maxRetries);
      } else {
        // Si el rol no corresponde a ningún grupo, ejecutar en todas las instancias
        return executeOnAllInstances(operation, maxRetries);
      }
    }
  }

  // Para operaciones normales (no executeOnAllInstances)
  const group = role ? getRoleGroup(role) : null;
  const queryCache = group ? queryCaches[group] : new Map(); // Cache temporal si no hay grupo

  let retries = 0;
  let lastError;

  while (retries < maxRetries) {
    try {
      // Intentar usar caché si está habilitado y hay un grupo específico
      if (useCache && group) {
        const cacheKey = `${operation.operation}-${
          operation.collection
        }-${JSON.stringify(operation.filter || {})}-${JSON.stringify(
          operation.data || {}
        )}`;
        const cachedItem = queryCache.get(cacheKey);

        if (cachedItem && Date.now() - cachedItem.timestamp < CACHE_TTL) {
          console.log(
            `Cache hit (${group}):`,
            cacheKey.substring(0, 50) + "..."
          );
          return cachedItem.result;
        }
      }

      // Si no hay caché o está desactualizado, ejecutar la operación
      const start = Date.now();
      const { client, db } = getRandomClient(group || undefined);

      const result = await executeMongoOperationOnDB(db, operation);
      const duration = Date.now() - start;

      console.log(
        `MongoDB Operation ejecutada (${group || "cualquier grupo"})`,
        {
          operation: operation.operation,
          collection: operation.collection,
          duration,
          result:
            typeof result === "object" && result !== null
              ? Array.isArray(result)
                ? `${result.length} documentos`
                : "documento único"
              : result,
        }
      );

      // Guardar en caché si está habilitado y hay un grupo específico
      if (useCache && group) {
        const cacheKey = `${operation.operation}-${
          operation.collection
        }-${JSON.stringify(operation.filter || {})}-${JSON.stringify(
          operation.data || {}
        )}`;
        queryCache.set(cacheKey, {
          timestamp: Date.now(),
          result: result,
        });
      }

      return result;
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
    `Error en operación MongoDB después de todos los reintentos (${
      group || "cualquier grupo"
    }):`,
    lastError
  );
  throw lastError;
}

// Función para cerrar todos los clientes (útil al finalizar el script)
export async function closeAllClients() {
  const closePromises = [];

  for (const group in mongoDBInstances) {
    for (const client of mongoDBInstances[group]) {
      closePromises.push(client.close());
    }
  }

  await Promise.all(closePromises);
  console.log("Todos los clientes MongoDB han sido cerrados");
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
export async function closeClient() {
  await closeAllClients();
}

// Función para detectar si es una operación de escritura
function isWriteOperation(operation: string): boolean {
  return [
    "insertOne",
    "insertMany",
    "updateOne",
    "updateMany",
    "deleteOne",
    "deleteMany",
    "replaceOne",
  ].includes(operation);
}

// Para compatibilidad con código existente que espera una función similar a query
const defaultExecuteOperation = async (
  operation: MongoOperation,
  useCache: boolean = false
) => {
  // Detectar si es una operación de escritura
  if (isWriteOperation(operation.operation)) {
    // Si es escritura, ejecutar en TODAS las instancias de TODOS los grupos
    return executeOnAllInstances(operation);
  } else {
    // Para operaciones de lectura, ejecutar en una instancia aleatoria
    return executeMongoDBOperation(operation, { useCache });
  }
};

const RDP03_DB_INSTANCES = {
  executeOperation: defaultExecuteOperation,
  executeMongoDBOperation,
  closeClient,
};

export default RDP03_DB_INSTANCES;
