import { RolesSistema } from "../../../../../interfaces/shared/RolesSistema";
import { bloquearRoles, obtenerIdsRoles } from "./bloquearRoles";
import RDP02_DB_INSTANCES from '../../../connectors/postgres';

/**
 * Desbloquea los roles especificados del sistema.
 * @param roles Array de roles a desbloquear. Si es null o vacío, desbloquea todos los roles.
 * @returns Void
 */
export async function desbloquearRoles(
  roles: RolesSistema[] | null = null
): Promise<void> {
  try {
    if (!roles || roles.length === 0) {
      console.log("Desbloqueando todos los roles del sistema...");
      const sql = `
            UPDATE "T_Bloqueo_Roles"
            SET "Bloqueo_Total" = false, 
                "Timestamp_Desbloqueo" = 0
          `;
      await RDP02_DB_INSTANCES.query(sql);
      console.log("Todos los roles han sido desbloqueados");
    } else {
      console.log(`Desbloqueando roles específicos: ${roles.join(", ")}...`);

      // Obtener los IDs correspondientes a los roles
      const roleIds = await obtenerIdsRoles(roles);

      if (roleIds.length === 0) {
        console.warn("No se encontraron IDs para los roles especificados");
        return;
      }

      const sql = `
            UPDATE "T_Bloqueo_Roles"
            SET "Bloqueo_Total" = false, 
                "Timestamp_Desbloqueo" = 0
            WHERE "Id_Bloqueo_Rol" = ANY($1)
          `;
      await RDP02_DB_INSTANCES.query(sql, [roleIds]);
      console.log(`Roles ${roles.join(", ")} han sido desbloqueados`);
    }
  } catch (error) {
    console.error("Error al desbloquear roles:", error);
    throw error;
  }
}

/**
 * Ejecuta una función mientras mantiene bloqueados ciertos roles del sistema,
 * garantizando que los roles se desbloqueen incluso si ocurre un error.
 * @param callback Función asíncrona a ejecutar mientras los roles están bloqueados
 * @param roles Array de roles a bloquear. Si es null o vacío, bloquea todos los roles.
 * @param tiempoBloqueoMinutos Tiempo de bloqueo en minutos
 * @returns El resultado de la función callback
 */
export async function ejecutarConRolesBloqueados<T>(
  callback: () => Promise<T>,
  roles: RolesSistema[] | null = null,
  tiempoBloqueoMinutos: number = 10
): Promise<T> {
  try {
    // Bloquear roles
    await bloquearRoles(roles, tiempoBloqueoMinutos);

    // Ejecutar la función
    return await callback();
  } finally {
    try {
      // Desbloquear roles sin importar lo que ocurra
      await desbloquearRoles(roles);
    } catch (unlockError) {
      console.error("Error al desbloquear roles:", unlockError);
    }
  }
}
