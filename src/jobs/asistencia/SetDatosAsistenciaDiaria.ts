import { DatosAsistenciaHoyIE20935 } from "../../interfaces/shared/Asistencia/DatosAsistenciaHoyIE20935";

import { verificarDiaEvento } from "../../core/databases/queries/RDP02/eventos/verificarDiaEvento";

import { obtenerComunicadosActivos } from "../../core/databases/queries/RDP02/comunicados/obtenerComunicadosActivos";
import { obtenerPersonalAdministrativoParaTomarAsistencia } from "../../core/databases/queries/RDP02/personal-administrativo/obtenerPersonalAdministrativoParaTomarAsistencia";
import { obtenerProfesoresPrimariaParaTomarAsistencia } from "../../core/databases/queries/RDP02/profesores-primaria/obtenerProfesoresPrimariaParaTomarAsistencia";
import { obtenerProfesoresSecundariaParaTomarAsistencia } from "../../core/databases/queries/RDP02/profesores-tutores-secundaria/obtenerProfesoresSecundariaParaTomarAsistencia";
import { obtenerHorariosGenerales } from "../../core/databases/queries/RDP02/horarios/obtenerHorariosGenerales";
import { obtenerHorariosEscolares } from "../../core/databases/queries/RDP02/horarios/obtenerHorariosEscolares";
import { guardarDatosAsistenciaEnBlobs } from "../../core/external/vercel/blobs/guardarDatosAsistenciaEnBlobs";
import { obtenerFechasActuales } from "../../core/utils/dates/obtenerFechasActuales";
import { obtenerFechasAñoEscolar } from "../../core/databases/queries/RDP02/fechas-importantes/obtenerFechasAñoEscolar";

import { obtenerSemanaDeGestion } from "../../core/databases/queries/RDP02/fechas-importantes/obtenerSemanaDeGestion";
import { verificarDentroSemanaGestion } from "../../core/utils/verificators/verificarDentroSemanaGestion";

import { closePool } from "../../core/databases/connectors/postgres";
import verificarFueraAñoEscolar from "../../core/utils/verificators/verificarDentroAñoEscolar";
import { obtenerAuxiliaresParaTomarAsistencia } from "../../core/databases/queries/RDP02/auxiliares/obtenerAuxiliaresParaTomarAsistencia";
import { actualizarArchivoDatosAsistenciaDiariosRespaldoEnGoogleDrive } from "../../core/external/google/drive/actualizarArchivoDatosAsistencia";
import { registrarAsistenciaAutoNullParaPersonalInactivo } from "../../core/databases/queries/RDP02/personales-para-toma-asistencia/registrarAsistenciaAutoNullParaPersonalInactivo";
import { obtenerVacacionesInterescolares } from "../../core/databases/queries/RDP02/vacaciones-interescolares/obtenerVacacionesInterescolares";

async function generarDatosAsistenciaDiaria(): Promise<DatosAsistenciaHoyIE20935> {
  // Obtener fechas actuales
  const { fechaUTC, fechaLocalPeru } = obtenerFechasActuales();

  // Verificar si es día de evento
  const esDiaEvento = await verificarDiaEvento(fechaLocalPeru);

  // Obtener fechas del año escolar
  const fechasAñoEscolar = await obtenerFechasAñoEscolar();

  // Verificar si estamos dentro del año escolar
  const fueraAñoEscolar = verificarFueraAñoEscolar(
    fechaLocalPeru,
    fechasAñoEscolar.Inicio_Año_Escolar,
    fechasAñoEscolar.Fin_Año_Escolar
  );

  // Obtener vacaciones interescolares - UNA SOLA VEZ
  const vacacionesInterescolares = await obtenerVacacionesInterescolares();

  // Obtener semana de gestión - UNA SOLA VEZ
  const semanaGestion = await obtenerSemanaDeGestion();
  const dentroSemanaGestion = verificarDentroSemanaGestion(
    fechaLocalPeru,
    semanaGestion
  );

  // Obtener comunicados activos para hoy
  const comunicados = await obtenerComunicadosActivos(fechaLocalPeru);

  // Obtener listas de personal, pasando fechas especiales
  const personalAdministrativo =
    await obtenerPersonalAdministrativoParaTomarAsistencia(
      fechaLocalPeru,
      vacacionesInterescolares,
      semanaGestion
    );

  const profesoresPrimaria =
    await obtenerProfesoresPrimariaParaTomarAsistencia();

  const profesoresSecundaria =
    await obtenerProfesoresSecundariaParaTomarAsistencia(
      fechaLocalPeru,
      vacacionesInterescolares,
      semanaGestion
    );

  const auxiliares = await obtenerAuxiliaresParaTomarAsistencia();

  // Obtener configuraciones de horarios con los nuevos parámetros
  const horariosGenerales = await obtenerHorariosGenerales(
    fechaLocalPeru,
    vacacionesInterescolares,
    semanaGestion
  );

  const horariosEscolares = await obtenerHorariosEscolares(fechaLocalPeru);

  // Construir el objeto de datos
  const datosAsistencia: DatosAsistenciaHoyIE20935 = {
    DiaEvento: esDiaEvento,
    FechaUTC: fechaUTC,
    FechaLocalPeru: fechaLocalPeru,
    FueraAñoEscolar: fueraAñoEscolar,
    Vacaciones_Interescolares: vacacionesInterescolares,
    Semana_De_Gestion: dentroSemanaGestion,
    ComunicadosParaMostrarHoy: comunicados,
    ListaDeAuxiliares: auxiliares,
    ListaDePersonalesAdministrativos: personalAdministrativo,
    ListaDeProfesoresPrimaria: profesoresPrimaria,
    ListaDeProfesoresSecundaria: profesoresSecundaria,
    HorariosLaboraresGenerales: horariosGenerales,
    HorariosEscolares: horariosEscolares,
  };

  return datosAsistencia;
}

async function main() {
  try {
    console.log("Iniciando generación de datos de asistencia diaria...");

    // Generar datos de asistencia normal
    const datosAsistencia = await generarDatosAsistenciaDiaria();

    // Guardar datos en Vercel Blob
    await guardarDatosAsistenciaEnBlobs(datosAsistencia);

    // Guardar datos en archivo de respaldo correspondiente en Google Drive y en la BD
    await actualizarArchivoDatosAsistenciaDiariosRespaldoEnGoogleDrive(
      datosAsistencia
    );

    // NUEVA FUNCIONALIDAD: Registrar asistencia automática como null para personal inactivo
    console.log(
      "Iniciando registro automático de asistencia para personal inactivo..."
    );
    try {
      const resultadoRegistroInactivos =
        await registrarAsistenciaAutoNullParaPersonalInactivo();
      console.log(
        "Registro automático de asistencia para personal inactivo completado:"
      );
      console.log(
        `- Total registros procesados: ${resultadoRegistroInactivos.totalRegistros}`
      );
      console.log(
        `- Nuevos registros creados: ${resultadoRegistroInactivos.registrosCreados}`
      );
      console.log(
        `- Registros existentes actualizados: ${resultadoRegistroInactivos.registrosActualizados}`
      );
    } catch (inactiveError) {
      // No interrumpimos el proceso principal por un error en este bloque try catch
      console.error(
        "Error al procesar asistencia automática para personal inactivo, pero continuando con el resto del proceso:",
        inactiveError
      );
    }

    // Imprimir en consola para verificación
    console.log(JSON.stringify(datosAsistencia, null, 2));

    console.log("Datos de asistencia generados y guardados correctamente.");
  } catch (error) {
    console.error("Error al generar datos de asistencia:", error);
    process.exit(1); // Terminar con código de error
  } finally {
    await closePool();
    console.log("Conexiones cerradas. Finalizando proceso...");
    process.exit(0); // Terminar con código de éxito
  }
}

main();
