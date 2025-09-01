import { RolesSistema } from "../../../../../interfaces/shared/RolesSistema";
import { verificarExistenciaRegistroDiario } from "../asistencias-diarias-unitarias/verficarExistenciaRegistroDiario";
import { registrarAsistenciaConValoresNull } from "./registrarAsistenciaAutoNullParaPersonalesActivosFaltantes";
import { verificarTablasPorRol } from "./verificarTablasPorRol";

export interface PersonalActivo {
  idUsuario: string;
  rol: RolesSistema;
  tablaMensualEntrada: string;
  tablaMensualSalida: string;
  campoId: string;
  campoIdUsuario: string;
  nombreCompleto: string;
  horaEntradaEsperada?: string;
  horaSalidaEsperada?: string;
}
export async function verificarYRegistrarAsistenciasIncompletas(
  personal: PersonalActivo[],
  fechaLocalPeru: Date
): Promise<{
  registrosEntradaCreados: number;
  registrosSalidaCreados: number;
  personalSinRegistroEntrada: PersonalActivo[];
  personalSinRegistroSalida: PersonalActivo[];
}> {
  // ✅ CORRECCIÓN: Usar métodos UTC
  const mes = fechaLocalPeru.getUTCMonth() + 1;  // Usar getUTCMonth()
  const dia = fechaLocalPeru.getUTCDate();       // Usar getUTCDate()

  console.log(`🔍 DEBUG - Procesando faltas para mes: ${mes}, día: ${dia}`);

  let registrosEntradaCreados = 0;
  let registrosSalidaCreados = 0;
  const personalSinRegistroEntrada: PersonalActivo[] = [];
  const personalSinRegistroSalida: PersonalActivo[] = [];

  // Verificar qué tablas existen realmente
  const tablasExistentes = await verificarTablasPorRol();

  // Verificar asistencias para cada persona
  for (const persona of personal) {
    // Verificar entradas
    const tablaEntradaReal = tablasExistentes.get(persona.tablaMensualEntrada);
    if (tablaEntradaReal) {
      try {
        const tieneEntrada = await verificarExistenciaRegistroDiario(
          tablaEntradaReal,
          persona.campoIdUsuario,
          persona.idUsuario,
          mes,
          dia,
          "Entradas"
        );

        if (!tieneEntrada) {
          // Registrar entrada con timestamp: null y desfaseSegundos: null
          await registrarAsistenciaConValoresNull(
            tablaEntradaReal,
            persona.campoIdUsuario,
            persona.idUsuario,
            mes,
            dia,
            "Entradas"
          );
          registrosEntradaCreados++;
          personalSinRegistroEntrada.push(persona);
        }
      } catch (error) {
        console.error(
          `Error al verificar entrada para ${persona.nombreCompleto} (${persona.idUsuario}):`,
          error
        );
      }
    } else {
      console.warn(
        `Tabla ${persona.tablaMensualEntrada} no existe en la base de datos. Omitiendo registro de entrada para ${persona.nombreCompleto}`
      );
    }

    // Verificar salidas
    const tablaSalidaReal = tablasExistentes.get(persona.tablaMensualSalida);
    if (tablaSalidaReal) {
      try {
        const tieneSalida = await verificarExistenciaRegistroDiario(
          tablaSalidaReal,
               persona.campoIdUsuario,
          persona.idUsuario,
          mes,
          dia,
          "Salidas"
        );

        if (!tieneSalida) {
          // Registrar salida con timestamp: null y desfaseSegundos: null
          await registrarAsistenciaConValoresNull(
            tablaSalidaReal,
                    persona.campoIdUsuario,
          persona.idUsuario,
            mes,
            dia,
            "Salidas"
          );
          registrosSalidaCreados++;
          personalSinRegistroSalida.push(persona);
        }
      } catch (error) {
        console.error(
          `Error al verificar salida para ${persona.nombreCompleto} (${persona.idUsuario}):`,
          error
        );
      }
    } else {
      console.warn(
        `Tabla ${persona.tablaMensualSalida} no existe en la base de datos. Omitiendo registro de salida para ${persona.nombreCompleto}`
      );
    }
  }

  return {
    registrosEntradaCreados,
    registrosSalidaCreados,
    personalSinRegistroEntrada,
    personalSinRegistroSalida,
  };
}
