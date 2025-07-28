import { RolesSistema } from "../../../../../interfaces/shared/RolesSistema";
import RDP02_DB_INSTANCES from "../../../connectors/postgres";

/**
 * Bloquea los roles especificados del sistema usando UPSERT.
 * @param roles Array de roles a bloquear. Si es null o vacío, bloquea todos los roles.
 * @param tiempoBloqueoMinutos Tiempo de bloqueo en minutos
 * @returns Void
 */
export async function bloquearRoles(
  roles: RolesSistema[] | null = null,
  tiempoBloqueoMinutos?: number
): Promise<void> {
  try {
    const timestamp = tiempoBloqueoMinutos
      ? Date.now() + tiempoBloqueoMinutos * 60 * 1000
      : 0;

    if (!roles || roles.length === 0) {
      console.log("Bloqueando todos los roles del sistema...");

      // Obtener todos los roles del enum
      const todosLosRoles = Object.values(RolesSistema).map((rol) =>
        String(rol)
      );
      console.log(
        `🔍 Debug - Todos los roles del enum: ${todosLosRoles.join(", ")}`
      );

      // UPSERT para cada rol
      for (const rol of todosLosRoles) {
        const upsertSql = `
          INSERT INTO "T_Bloqueo_Roles" ("Rol", "Bloqueo_Total", "Timestamp_Desbloqueo")
          VALUES ($1, true, $2)
          ON CONFLICT ("Rol") 
          DO UPDATE SET 
            "Bloqueo_Total" = true,
            "Timestamp_Desbloqueo" = $2
        `;
        await RDP02_DB_INSTANCES.query(upsertSql, [rol, timestamp]);
      }

      console.log(
        `✅ Todos los roles han sido bloqueados por ${tiempoBloqueoMinutos} minutos`
      );
    } else {
      console.log(`Bloqueando roles específicos: ${roles.join(", ")}...`);

      // Convertir roles a string
      const rolesArray = roles.map((rol) => String(rol));
      console.log(
        `🔍 Debug - Roles convertidos a string: ${rolesArray.join(", ")}`
      );

      // UPSERT para cada rol específico
      for (const rol of rolesArray) {
        const upsertSql = `
          INSERT INTO "T_Bloqueo_Roles" ("Rol", "Bloqueo_Total", "Timestamp_Desbloqueo")
          VALUES ($1, true, $2)
          ON CONFLICT ("Rol") 
          DO UPDATE SET 
            "Bloqueo_Total" = true,
            "Timestamp_Desbloqueo" = $2
        `;
        await RDP02_DB_INSTANCES.query(upsertSql, [rol, timestamp]);
      }

      console.log(
        `✅ Roles ${roles.join(
          ", "
        )} han sido bloqueados por ${tiempoBloqueoMinutos} minutos`
      );
    }
  } catch (error) {
    console.error("❌ Error al bloquear roles:", error);
    throw error;
  }
}
