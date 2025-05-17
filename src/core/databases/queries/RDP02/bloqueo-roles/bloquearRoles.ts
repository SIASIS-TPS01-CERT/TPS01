import { RolesSistema } from "../../../../../interfaces/shared/RolesSistema";
import RDP02_DB_INSTANCES from '../../../connectors/postgres';

/**
 * Convierte roles del enum RolesSistema a sus correspondientes IDs en la tabla T_Bloqueo_Roles
 * @param roles Array de roles del enum RolesSistema
 * @returns Array de IDs de roles
 */
export async function obtenerIdsRoles(
  roles: RolesSistema[]
): Promise<number[]> {
  try {
    const rolesStr = roles.map((r) => `'${r}'`).join(", ");
    const sql = `
        SELECT "Id_Bloqueo_Rol"
        FROM "T_Bloqueo_Roles"
        WHERE "Rol" IN (${rolesStr})
      `;

    const result = await RDP02_DB_INSTANCES.query(sql);
    return result.rows.map((row: any) => row.Id_Bloqueo_Rol);
  } catch (error) {
    console.error("Error al obtener IDs de roles:", error);
    throw error;
  }
}

/**
 * Bloquea los roles especificados del sistema.
 * @param roles Array de roles a bloquear. Si es null o vacío, bloquea todos los roles.
 * @param tiempoBloqueoMinutos Tiempo de bloqueo en minutos
 * @returns Void
 */
export async function bloquearRoles(
  roles: RolesSistema[] | null = null,
  tiempoBloqueoMinutos: number = 10
): Promise<void> {
  try {
    const timestamp = Date.now() + tiempoBloqueoMinutos * 60 * 1000;

    if (!roles || roles.length === 0) {
      console.log("Bloqueando todos los roles del sistema...");
      const sql = `
          UPDATE "T_Bloqueo_Roles"
          SET "Bloqueo_Total" = true, 
              "Timestamp_Desbloqueo" = $1
        `;
      await RDP02_DB_INSTANCES.query(sql, [timestamp]);
      console.log(
        `Todos los roles han sido bloqueados por ${tiempoBloqueoMinutos} minutos`
      );
    } else {
      console.log(`Bloqueando roles específicos: ${roles.join(", ")}...`);

      // Obtener los IDs correspondientes a los roles
      const roleIds = await obtenerIdsRoles(roles);

      if (roleIds.length === 0) {
        console.warn("No se encontraron IDs para los roles especificados");
        return;
      }

      const sql = `
          UPDATE "T_Bloqueo_Roles"
          SET "Bloqueo_Total" = true, 
              "Timestamp_Desbloqueo" = $1
          WHERE "Id_Bloqueo_Rol" = ANY($2)
        `;
      await RDP02_DB_INSTANCES.query(sql, [timestamp, roleIds]);
      console.log(
        `Roles ${roles.join(
          ", "
        )} han sido bloqueados por ${tiempoBloqueoMinutos} minutos`
      );
    }
  } catch (error) {
    console.error("Error al bloquear roles:", error);
    throw error;
  }
}
