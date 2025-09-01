// src/constants/MONGODB_CONFIG.ts

/**
 * Configuraciones para conexiones MongoDB
 */

// Tiempo de conexión inicial (en milisegundos)
export const MONGO_CONNECTION_TIMEOUT =
  process.env.MONGO_CONNECTION_TIMEOUT || "10000";

// Tiempo para selección de servidor (en milisegundos)
export const MONGO_SERVER_SELECTION_TIMEOUT =
  process.env.MONGO_SERVER_SELECTION_TIMEOUT || "5000";

// Número máximo de conexiones en el pool
export const MONGO_MAX_POOL_SIZE = process.env.MONGO_MAX_POOL_SIZE || "10";

// Número mínimo de conexiones en el pool
export const MONGO_MIN_POOL_SIZE = process.env.MONGO_MIN_POOL_SIZE || "2";

// Tiempo de vida máximo de una conexión inactiva (en milisegundos)
export const MONGO_MAX_IDLE_TIME = process.env.MONGO_MAX_IDLE_TIME || "30000";

// Frecuencia de heartbeat (en milisegundos)
export const MONGO_HEARTBEAT_FREQUENCY =
  process.env.MONGO_HEARTBEAT_FREQUENCY || "10000";
