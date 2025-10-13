import { T_Aulas } from "@prisma/client";
import {
  CONTROL_ASISTENCIA_DE_SALIDA_PRIMARIA,
  CONTROL_ASISTENCIA_DE_SALIDA_SECUNDARIA,
} from "../../../../constants/ASISTENCIA_ENTRADA_SALIDA_ESCOLAR";
import { ArchivoAsistenciaEscolarDiaria } from "../../../../interfaces/shared/Asistencia/ArchivoAsistenciaEscolarDiaria";
import { NivelEducativo } from "../../../../interfaces/shared/NivelEducativo";
import {
  EstudianteActivoSecundaria,
  RegistroEstudianteSecundariaRedis,
} from "../../../../jobs/asistenciaEscolar/SetAsistenciasYFaltasEstudiantesSecundaria";
import { ModoRegistro } from "../../../../interfaces/shared/ModoRegistroPersonal";
import { AsistenciaEscolarDeUnDia } from "../../../../interfaces/shared/AsistenciasEscolares";

interface ParametrosConstruccionAsistencias {
  estudiantesActivos: EstudianteActivoSecundaria[];
  registrosRedis: RegistroEstudianteSecundariaRedis[];
  nivel: NivelEducativo;
  fechaActual: Date;
}

/**
 * Construye el objeto completo de asistencias escolares del día
 * desde los datos en memoria (registros de Redis y estudiantes activos)
 *
 * 🆕 ACTUALIZADO: Ahora agrupa por Grado → Sección e incluye información del aula
 */
export function construirAsistenciasEscolaresDelDia(
  params: ParametrosConstruccionAsistencias
): ArchivoAsistenciaEscolarDiaria {
  const { estudiantesActivos, registrosRedis, nivel, fechaActual } = params;

  console.log(`\n📝 Construyendo archivo de asistencias para ${nivel}...`);

  // Determinar si se debe incluir control de salida
  const incluirSalida =
    nivel === NivelEducativo.SECUNDARIA
      ? CONTROL_ASISTENCIA_DE_SALIDA_SECUNDARIA
      : CONTROL_ASISTENCIA_DE_SALIDA_PRIMARIA;

  console.log(
    `   Control de salida: ${incluirSalida ? "ACTIVADO" : "DESACTIVADO"}`
  );

  // Crear mapa de registros por estudiante y modo
  const mapaRegistros = new Map<string, Map<ModoRegistro, number>>();

  for (const registro of registrosRedis) {
    if (!mapaRegistros.has(registro.idEstudiante)) {
      mapaRegistros.set(registro.idEstudiante, new Map());
    }

    const registrosEstudiante = mapaRegistros.get(registro.idEstudiante)!;

    // Solo agregar si no existe o si el nuevo tiene menor desfase (llegó más temprano)
    const desfaseActual = registrosEstudiante.get(registro.modoRegistro);
    if (
      desfaseActual === undefined ||
      registro.desfaseSegundos < desfaseActual
    ) {
      registrosEstudiante.set(registro.modoRegistro, registro.desfaseSegundos);
    }
  }

  // 🆕 NUEVO: Agrupar estudiantes por grado y sección
  type GrupoGradoSeccion = {
    aula: T_Aulas;
    estudiantes: EstudianteActivoSecundaria[];
  };

  const estudiantesPorGradoSeccion = new Map<string, GrupoGradoSeccion>();

  for (const estudiante of estudiantesActivos) {
    const claveGrupo = `${estudiante.grado}-${estudiante.seccion}`;

    if (!estudiantesPorGradoSeccion.has(claveGrupo)) {
      estudiantesPorGradoSeccion.set(claveGrupo, {
        aula: estudiante.aula,
        estudiantes: [],
      });
    }

    estudiantesPorGradoSeccion.get(claveGrupo)!.estudiantes.push(estudiante);
  }

  console.log(`   📊 Aulas encontradas: ${estudiantesPorGradoSeccion.size}`);

  // 🆕 NUEVO: Construir estructura por Grado → Sección
  const archivoPorGradoSeccion: ArchivoAsistenciaEscolarDiaria = {};

  for (const [claveGrupo, grupo] of estudiantesPorGradoSeccion) {
    const { aula, estudiantes } = grupo;
    const grado = aula.Grado.toString();
    const seccion = aula.Seccion;

    console.log(
      `   📋 Procesando ${grado}${seccion}: ${estudiantes.length} estudiantes`
    );

    // Inicializar grado si no existe
    if (!archivoPorGradoSeccion[grado]) {
      archivoPorGradoSeccion[grado] = {};
    }

    // Construir asistencias para cada estudiante de esta aula
    const asistenciasDelAula: Record<string, AsistenciaEscolarDeUnDia> = {};

    for (const estudiante of estudiantes) {
      const registrosEstudiante = mapaRegistros.get(estudiante.idEstudiante);

      const asistencia: AsistenciaEscolarDeUnDia = {
        [ModoRegistro.Entrada]: null,
      };

      if (registrosEstudiante) {
        // Tiene registro de entrada
        const desfaseEntrada = registrosEstudiante.get(ModoRegistro.Entrada);
        if (desfaseEntrada !== undefined) {
          asistencia[ModoRegistro.Entrada] = {
            DesfaseSegundos: desfaseEntrada,
          };
        } else {
          // No tiene entrada = falta
          asistencia[ModoRegistro.Entrada] = {
            DesfaseSegundos: null,
          };
        }

        // Incluir salida solo si está activado el control
        if (incluirSalida) {
          const desfaseSalida = registrosEstudiante.get(ModoRegistro.Salida);
          if (desfaseSalida !== undefined) {
            asistencia[ModoRegistro.Salida] = {
              DesfaseSegundos: desfaseSalida,
            };
          } else {
            // No tiene salida = falta de salida
            asistencia[ModoRegistro.Salida] = {
              DesfaseSegundos: null,
            };
          }
        }
      } else {
        // No tiene ningún registro = falta completa
        asistencia[ModoRegistro.Entrada] = {
          DesfaseSegundos: null,
        };

        if (incluirSalida) {
          asistencia[ModoRegistro.Salida] = {
            DesfaseSegundos: null,
          };
        }
      }

      asistenciasDelAula[estudiante.idEstudiante] = asistencia;
    }

    // Agregar a la estructura por grado/sección
    archivoPorGradoSeccion[grado][seccion] = {
      Aula: aula,
      ListaAsistenciasEscolares: asistenciasDelAula,
    };
  }

  console.log(
    `✅ Archivo construido con ${estudiantesActivos.length} estudiantes`
  );

  // Estadísticas globales
  let conEntrada = 0;
  let sinEntrada = 0;
  let conSalida = 0;
  let sinSalida = 0;

  for (const grado of Object.values(archivoPorGradoSeccion)) {
    for (const seccion of Object.values(grado)) {
      for (const asistencia of Object.values(
        seccion.ListaAsistenciasEscolares
      )) {
        if (asistencia[ModoRegistro.Entrada]?.DesfaseSegundos !== null) {
          conEntrada++;
        } else {
          sinEntrada++;
        }

        if (incluirSalida) {
          if (asistencia[ModoRegistro.Salida]?.DesfaseSegundos !== null) {
            conSalida++;
          } else {
            sinSalida++;
          }
        }
      }
    }
  }

  console.log(`   📥 Con entrada: ${conEntrada} | Sin entrada: ${sinEntrada}`);
  if (incluirSalida) {
    console.log(`   📤 Con salida: ${conSalida} | Sin salida: ${sinSalida}`);
  }

  return archivoPorGradoSeccion;
}
