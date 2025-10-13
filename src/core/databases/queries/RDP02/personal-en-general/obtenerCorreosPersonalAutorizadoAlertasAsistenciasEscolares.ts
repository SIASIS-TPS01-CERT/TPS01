import { NivelEducativo } from "../../../../../interfaces/shared/NivelEducativo";
import RDP02_DB_INSTANCES from "../../../connectors/postgres";
import { obtenerDatosEstudiantesYAulasDesdeGoogleDrive } from "../../RDP01/obtenerDatosEstudiantesYAulasDesdeGoogleDrive";
import { ConfiguracionesReportes } from "../ajustes-generales/obtenerConfiguracionesReportesEscolares";

export interface Destinatario {
  id: string;
  nombres: string;
  apellidos: string;
  correo: string;
  rol: string;
  idAula?: string; // Solo para tutores y profesores
  grado?: number;
  seccion?: string;
}

export interface DestinatariosCorreos {
  directivos: Destinatario[];
  tutores: Destinatario[]; // Solo secundaria
  auxiliares: Destinatario[]; // Solo secundaria
  profesoresPrimaria: Destinatario[]; // Solo primaria
}

export async function obtenerDestinatariosCorreos(
  nivel: NivelEducativo,
  configuraciones: ConfiguracionesReportes,
  aulasAfectadas: Set<string>
): Promise<DestinatariosCorreos> {
  try {
    const destinatarios: DestinatariosCorreos = {
      directivos: [],
      tutores: [],
      auxiliares: [],
      profesoresPrimaria: [],
    };

    // Obtener directivos (si está activado para faltas o tardanzas)
    if (
      configuraciones.enviarCorreoFaltasDirectivos ||
      configuraciones.enviarCorreoTardanzasDirectivos
    ) {
      const sqlDirectivos = `
        SELECT 
          "Id_Directivo"::text as id,
          "Nombres" as nombres,
          "Apellidos" as apellidos,
          "Correo_Electronico" as correo
        FROM "T_Directivos"
        WHERE "Correo_Electronico" IS NOT NULL
      `;

      const resultDirectivos = await RDP02_DB_INSTANCES.query(sqlDirectivos);

      destinatarios.directivos = resultDirectivos.rows.map((row: any) => ({
        id: row.id,
        nombres: row.nombres,
        apellidos: row.apellidos,
        correo: row.correo,
        rol: "Directivo",
      }));

      console.log(`   ✅ Directivos: ${destinatarios.directivos.length}`);
    }

    // ============================================================
    // 🆕 NUEVA LÓGICA: Obtener aulas desde Google Drive
    // ============================================================

    if (aulasAfectadas.size === 0) {
      console.log(
        `   ℹ️  No hay aulas afectadas, omitiendo tutores/profesores`
      );
      return destinatarios;
    }

    console.log(`   🔄 Obteniendo datos de aulas desde Google Drive...`);

    // Obtener datos desde Google Drive
    const { aulas: aulasMap } =
      await obtenerDatosEstudiantesYAulasDesdeGoogleDrive(nivel);

    console.log(`   ✅ Aulas obtenidas: ${aulasMap.size}`);

    // Filtrar solo las aulas afectadas
    const aulasAfectadasData = Array.from(aulasAfectadas)
      .map((idAula) => aulasMap.get(idAula))
      .filter((aula) => aula !== undefined);

    console.log(
      `   📋 Aulas afectadas con datos: ${aulasAfectadasData.length}`
    );

    if (nivel === NivelEducativo.SECUNDARIA) {
      // Obtener auxiliares
      if (
        configuraciones.enviarCorreoFaltasAuxiliares ||
        configuraciones.enviarCorreoTardanzasAuxiliares
      ) {
        const sqlAuxiliares = `
          SELECT 
            "Id_Auxiliar" as id,
            "Nombres" as nombres,
            "Apellidos" as apellidos,
            "Correo_Electronico" as correo
          FROM "T_Auxiliares"
          WHERE "Correo_Electronico" IS NOT NULL
            AND "Estado" = true
        `;

        const resultAuxiliares = await RDP02_DB_INSTANCES.query(sqlAuxiliares);

        destinatarios.auxiliares = resultAuxiliares.rows.map((row: any) => ({
          id: row.id,
          nombres: row.nombres,
          apellidos: row.apellidos,
          correo: row.correo,
          rol: "Auxiliar",
        }));

        console.log(`   ✅ Auxiliares: ${destinatarios.auxiliares.length}`);
      }

      // Obtener tutores de las aulas afectadas
      if (
        (configuraciones.enviarCorreoFaltasTutores ||
          configuraciones.enviarCorreoTardanzasTutores) &&
        aulasAfectadasData.length > 0
      ) {
        // Obtener IDs únicos de tutores desde las aulas
        const idsTutores = [
          ...new Set(
            aulasAfectadasData
              .map((aula) => aula!.Id_Profesor_Secundaria)
              .filter((id): id is string => id !== null)
          ),
        ];

        if (idsTutores.length > 0) {
          // Obtener datos de tutores desde PostgreSQL
          const sqlTutores = `
            SELECT 
              "Id_Profesor_Secundaria" as id,
              "Nombres" as nombres,
              "Apellidos" as apellidos,
              "Correo_Electronico" as correo
            FROM "T_Profesores_Secundaria"
            WHERE "Id_Profesor_Secundaria" = ANY($1)
              AND "Correo_Electronico" IS NOT NULL
              AND "Estado" = true
          `;

          const resultTutores = await RDP02_DB_INSTANCES.query(sqlTutores, [
            idsTutores,
          ]);

          // Asociar cada tutor con su aula
          const tutoresMap = new Map<string, any>(
            resultTutores.rows.map((row: any) => [row.id, row])
          );

          for (const aula of aulasAfectadasData) {
            if (!aula || !aula.Id_Profesor_Secundaria) continue;

            const tutor = tutoresMap.get(aula.Id_Profesor_Secundaria);
            if (tutor) {
              destinatarios.tutores.push({
                id: tutor.id as string,
                nombres: tutor.nombres as string,
                apellidos: tutor.apellidos as string,
                correo: tutor.correo as string,
                rol: "Tutor",
                idAula: aula.Id_Aula,
                grado: aula.Grado,
                seccion: aula.Seccion,
              });
            }
          }

          console.log(`   ✅ Tutores: ${destinatarios.tutores.length}`);
        }
      }
    } else {
      // PRIMARIA - Obtener profesores de las aulas afectadas
      if (
        (configuraciones.enviarCorreoFaltasProfesores ||
          configuraciones.enviarCorreoTardanzasProfesores) &&
        aulasAfectadasData.length > 0
      ) {
        // Obtener IDs únicos de profesores desde las aulas
        const idsProfesores = [
          ...new Set(
            aulasAfectadasData
              .map((aula) => aula!.Id_Profesor_Primaria)
              .filter((id): id is string => id !== null)
          ),
        ];

        if (idsProfesores.length > 0) {
          // Obtener datos de profesores desde PostgreSQL
          const sqlProfesores = `
            SELECT 
              "Id_Profesor_Primaria" as id,
              "Nombres" as nombres,
              "Apellidos" as apellidos,
              "Correo_Electronico" as correo
            FROM "T_Profesores_Primaria"
            WHERE "Id_Profesor_Primaria" = ANY($1)
              AND "Correo_Electronico" IS NOT NULL
              AND "Estado" = true
          `;

          const resultProfesores = await RDP02_DB_INSTANCES.query(
            sqlProfesores,
            [idsProfesores]
          );

          // Asociar cada profesor con su aula
          const profesoresMap = new Map<string, any>(
            resultProfesores.rows.map((row: any) => [row.id, row])
          );

          for (const aula of aulasAfectadasData) {
            if (!aula || !aula.Id_Profesor_Primaria) continue;

            const profesor = profesoresMap.get(aula.Id_Profesor_Primaria);
            if (profesor) {
              destinatarios.profesoresPrimaria.push({
                id: profesor.id as string,
                nombres: profesor.nombres as string,
                apellidos: profesor.apellidos as string,
                correo: profesor.correo as string,
                rol: "Profesor",
                idAula: aula.Id_Aula,
                grado: aula.Grado,
                seccion: aula.Seccion,
              });
            }
          }

          console.log(
            `   ✅ Profesores: ${destinatarios.profesoresPrimaria.length}`
          );
        }
      }
    }

    return destinatarios;
  } catch (error) {
    console.error("❌ Error obteniendo destinatarios:", error);
    throw error;
  }
}
