package data_etl;

import java.sql.Connection;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.joda.time.DateTime;
import org.joda.time.Minutes;
import org.joda.time.Seconds;

import utilities.CLogger;

public class CMain {
	private static Options options;
	
	static{
		options = new Options();
		
		options.addOption("cge", "cg-entidades", true, "importa el catalogo de entidades por ejericio");
		options.addOption("cpe", "cp-estructuras", true, "importa el catalogo de estructuras por ejericio");
		options.addOption("cgfun", "cg-funciones", true, "importa el catalogo de funciones por ejericio");
		options.addOption("cgu", "cg-unidadesejecutoras", false, "importa el catalogo de unidades ejecutoras por ejercicio");
		options.addOption("cgd", "cg-departamentos", false, "importa el catalogo de departamentos");
		options.addOption("cgf", "cg-fuentes", false, "importa el catalogo de fuentes por ejericio");
		options.addOption("cgg", "cg-geograficos", false, "importa el catalogo de geograficos por ejericio");
		options.addOption("cgr", "cg-regiones", false, "importa el catalogo de regiones por ejericio");
		options.addOption("cpgg", "cp-grupos-gasto", false, "importa el catalogo de grupos gasto por ejericio");
		options.addOption("cpsg", "cp-subgrupos-gasto", false, "importa el catalogo de subgrupos gasto por ejericio");
		options.addOption("cpr", "cp-renglones", true, "importa el catalogo de renglones por ejericio");
		options.addOption("pgi_ft_em", "pgi_ft_em", false, "calcula los indices de proyeccion del gasto de los ultimos 5 ejercicios de las fuentes tributarias por entidad y por mes" );
		options.addOption("acc", "aprobado-cc", true, "calcula el total de las aprobacion de copep segun solicitudes y anticipos");
		options.addOption("pvi", "presupuesto-vigente", false, "calcula el presupuesto vigente por entidad");
		options.addOption("ggem", "gasto-geo", true, "calcula el gasto por geografico, ejercicio y mes");
		options.addOption("gas", "gasto-ejercicio", true, "copia el gasto con detalle");
		options.addOption("gac", "gasto-ejercicio-actual", false, "copia el gasto actul con detalle");
		options.addOption("ant", "anticipos-cuota", true, "copia los anticipos de cuota");
		options.addOption("tn_ejes", "tn-ejes", false, "calcula los datos de los ejes del TN");
		options.addOption("tn_ent", "tn-entidades", false, "calcula los datos de las entidades del TN");
		options.addOption("tn_ef", "tn-estructuras-financieras", false, "calcula las estructuras financieras del TN");
		options.addOption("u_medida", "unidades-medida", false, "Carga ");
		options.addOption("te_estado_cuentas", "tesoreria-estado-cuentas", true, "importa los estados de cuenta iniciales de las cuentas de tesoreria ");
		options.addOption("update_all","update-all",false,"Cargar todas las tablas a MemSQL");
		options.addOption("update_all_des","update-all-des",false,"Cargar todas las tablas descentralizadas a MemSQL");
		options.addOption( "h", "help", false, "muestra este listado de opciones" );
	}
	
	final static  CommandLineParser parser = new DefaultParser();
	
	 public static void main(String[] args) throws Exception {
		 DateTime start = new DateTime();
		 CommandLine cline = parser.parse( options, args );
		 if (CHive.connect()){
			 Connection conn = CHive.getConnection();
			 if(cline.hasOption("proyeccion-gasto-indices-fuentes-tributarias-entidad-mes")){
				 CLogger.writeConsole("Inicio de calculo de indices de las fuentes tributarias por entidad por mes...");
				 if(CProyeccionGasto.calculateProyeccionGastoFuentesTributarias_EntidadMes(conn, "sicoinprod"))
					 CLogger.writeConsole("Indices de Proyeccion del Gasto, calculados con exito");
			 }
			 else if(cline.hasOption("anticipos-cuota")){
				 boolean historico = cline.getOptionValue("ant")!=null && cline.getOptionValue("ant").compareTo("true")==0;
				 CLogger.writeConsole("Inicio de copia de anticipos de cuotas..." + (historico ? "(historico)" : ""));
				 if(CAnticipo.loadAnticipos(conn,historico, "sicoinprod") )
					 CLogger.writeConsole("Anticipos copiados con exito");
			 }
			 else if(cline.hasOption("gasto-ejercicio")){
				 boolean historico = cline.getOptionValue("gas")!=null && cline.getOptionValue("gas").compareTo("true")==0;
				 CLogger.writeConsole("Inicio de copia del gasto por ejercicio, mes..." + (historico ? "(historico)" : ""));
				 if(CGasto.fullGastoHistorico(conn,historico, "sicoinprod") && CFechaActualizacionData.UpdateLoadDate("ejecucionpresupuestaria"))
					 CLogger.writeConsole("Gasto copiado con exito");
			 }
			 else if(cline.hasOption("gasto-ejercicio-actual")){
				 CLogger.writeConsole("Inicio de copia del gasto del ejercicio actual" );
				 if(CGastoActual.LoadGastoActual(conn, "sicoinprod"))
					 CLogger.writeConsole("Gasto actual copiado con exito");
			 }
			 else if(cline.hasOption("cg-entidades")){
				 boolean historico = cline.getOptionValue("cge")!=null && cline.getOptionValue("cge").compareTo("true")==0;
				 CLogger.writeConsole("Inicio de importacion de catalogo de entidades por ejercicio...");
				 if(CEntidades.loadEntidades(conn,historico, "sicoinprod"))
					 CLogger.writeConsole("Entidades por Ejercicio importadas con exito");
			 }
			 else if(cline.hasOption("cp-estructuras")){
				 int ejercicio = cline.getOptionValue("cpe")!=null ? Integer.parseInt(cline.getOptionValue("cpe")) : DateTime.now().getYear();
				 CLogger.writeConsole("Inicio de importacion de catalogo de estructuras por ejercicio...");
				 if(CEstructuras.loadEstructuras(conn, "sicoinprod", ejercicio))
					 CLogger.writeConsole("Estructuras por Ejercicio importadas con exito");
			 }
			 else if(cline.hasOption("cg-funciones")){
				 int ejercicio = cline.getOptionValue("cgfun")!=null ? Integer.parseInt(cline.getOptionValue("cgfun")) : DateTime.now().getYear();
				 CLogger.writeConsole("Inicio de importacion de catalogo de funciones por ejercicio...");
				 if(CFunciones.loadFunciones(conn, "sicoinprod", ejercicio))
					 CLogger.writeConsole("Funciones por Ejercicio importadas con exito");
			 }
			 else if(cline.hasOption("cg-unidadesejecutoras")){
				 CLogger.writeConsole("Inicio de importacion de catalogo de unidades ejecutoras por ejercicio...");
				 if(CUnidadesEjecutoras.loadUnidadesEjecutoras(conn, "sicoinprod"))
					 CLogger.writeConsole("Unidades Ejecutoras por Ejercicio importadas con exito");
			 }
			 else if(cline.hasOption("cg-departamentos")){
				 CLogger.writeConsole("Inicio de importacion de catalogo de departamentos...");
				 if(CDepartamentos.loadDepartamentos(conn))
					 CLogger.writeConsole("Departamentos importados con exito");
			 }
			 else if(cline.hasOption("cg-fuentes")){
				 CLogger.writeConsole("Inicio de importacion de catalogo de fuentes por ejercicio...");
				 if(CFuentes.loadFuentes(conn, "sicoinprod"))
					 CLogger.writeConsole("Fuentes importadas con exito");
			 }
			 else if(cline.hasOption("cg-geograficos")){
				 CLogger.writeConsole("Inicio de importacion de catalogo de geograficos por ejercicio...");
				 if(CGeograficos.loadGeograficos(conn, "sicoinprod"))
					 CLogger.writeConsole("Geograficos importados con exito");
			 }
			 else if(cline.hasOption("cg-regiones")){
				 CLogger.writeConsole("Inicio de importacion de catalogo de regiones por ejercicio...");
				 if(CRegiones.loadRegiones(conn, "sicoinprod"))
					 CLogger.writeConsole("Regiones importadas con exito");
			 }
			 else if(cline.hasOption("cp-grupos-gasto")){
				 CLogger.writeConsole("Inicio de importacion de catalogo de grupos gastos por ejercicio...");
				 if(CGrupoGasto.loadGruposGasto(conn, "sicoinprod"))
					 CLogger.writeConsole("Grupos gasto importadas con exito");
			 }
			 else if(cline.hasOption("cp-subgrupos-gasto")){
				 CLogger.writeConsole("Inicio de importacion de catalogo de subgrupos gastos por ejercicio...");
				 if(CSubgrupoGasto.loadSubgruposGasto(conn, "sicoinprod"))
					 CLogger.writeConsole("Subgrupos gasto importadas con exito");
			 }
			 else if(cline.hasOption("cp-renglones")){
				 boolean historico = cline.getOptionValue("cpr")!=null && cline.getOptionValue("cpr").compareTo("true")==0;
				 CLogger.writeConsole("Inicio de importacion de catalogo de renglones por ejercicio..." + (historico ? "(historico)" : ""));
				 if(CRenglones.loadRenglones(conn,historico, "sicoinprod"))
					 CLogger.writeConsole("Renglones importados con exito");
			 }
			 else if (cline.hasOption("aprobado-cc")){
				 boolean historico = cline.getOptionValue("acc")!=null && cline.getOptionValue("acc").compareTo("true")==0;
				 CLogger.writeConsole("Inicio de importacion de aprobaciones COPEP.."  + (historico ? "(historico)" : ""));
				 if(CAprobado.loadAprobados(conn,historico, "sicoinprod"))
					 CLogger.writeConsole("Aprobaciones importadas con exito");
			 }
			 else if (cline.hasOption("presupuesto-vigente")){
				 CLogger.writeConsole("Inicio de importacion de presupuesto vigente por entidad..");
				 if(CVigente.loadVigente(conn, "sicoinprod"))
					 CLogger.writeConsole("Vigente importados con exito");
			 }
			 else if (cline.hasOption("gasto-geografico-ejercicio-mes")){
				 boolean historico = cline.getOptionValue("ggem")!=null && cline.getOptionValue("ggem").compareTo("true")==0;
				 CLogger.writeConsole("Inicio de importacion de gasto por geografico, ejercicio y mes.." + (historico ? "(historico)" : ""));
				 if(CGasto.loadGastoPorEjercicioMesGeografico(conn,historico, "sicoinprod"))
					 CLogger.writeConsole("Gasto por geografico, ejercicio y mes importados con exito");
			 }
			 else if(cline.hasOption("tn-ejes")){
				 CLogger.writeConsole("Inicio calculos financieros de ejes del triangulo norte...");
				 if(CTrianguloNorte.loadEjesTrianguloNorte())
					 CLogger.writeConsole("Datos de Ejes del Triangulo Norte, calculados con exito");
			 }
			 else if(cline.hasOption("tn-entidades")){
				 CLogger.writeConsole("Inicio calculos financieros de entidades del triangulo norte...");
				 if(CTrianguloNorte.loadEntidadesTrianguloNorte())
					 CLogger.writeConsole("Datos de Entidades del Triangulo Norte, calculados con exito");
			 }
			 else if(cline.hasOption("tn-estructuras-financieras")){
				 CLogger.writeConsole("Inicio calculos financieros de estructuras de financiamiento del triangulo norte...");
				 if(CTrianguloNorte.loadEstructurasFinanciamiento())
					 CLogger.writeConsole("Datos de Estructuras de financiamiento del Triangulo Norte, calculados con exito");
			 }
			 else if(cline.hasOption("unidades-medida")){
				 CLogger.writeConsole("Inicio de carga de unidades de medida...");
				 if(CUnidadMedida.loadUnidadesMedida(conn, false, "sicoinprod"))
					 CLogger.writeConsole("Datos de unidades de medida cargados con exito");
			 }
			 else if(cline.hasOption("tesoreria-estado-cuentas")){
				 int ejercicio = cline.getOptionValue("te_estado_cuentas")!=null ? Integer.parseInt(cline.getOptionValue("te_estado_cuentas")) : DateTime.now().getYear();
				 CLogger.writeConsole("Inicio de importacion de la tabla te_estado_cuentas...");
				 if(CTesoreriaCuenta.loadEstadoCuentas(conn, "sicoinprod", ejercicio))
					 CLogger.writeConsole("Estads de Cuenta importados con exito");
			 }
			 else if (cline.hasOption("update-all")){
				 int ejercicio = cline.getOptionValue("update_all")!=null ? Integer.parseInt(cline.getOptionValue("update_all")) : DateTime.now().getYear();
				 CLogger.writeConsole("Inicio de importacion de todos las tablas.");
				 if(CAprobado.loadAprobados(conn,false, "sicoinprod") && 
						CAnticipo.loadAnticipos(conn,false, "sicoinprod") &&
						CEntidades.loadEntidades(conn,false, "sicoinprod") &&
						CEstructuras.loadEstructuras(conn, "sicoinprod", ejercicio) &&
						CFunciones.loadFunciones(conn, "sicoinprod", ejercicio) &&
						CFuentes.loadFuentes(conn, "sicoinprod") &&
						CGrupoGasto.loadGruposGasto(conn, "sicoinprod") &&
						CProyeccionGasto.calculateProyeccionGastoFuentesTributarias_EntidadMes(conn, "sicoinprod") &&
						CRenglones.loadRenglones(conn,false, "sicoinprod") &&
						CSubgrupoGasto.loadSubgruposGasto(conn, "sicoinprod") &&
						CUnidadesEjecutoras.loadUnidadesEjecutoras(conn, "sicoinprod") &&
						CVigente.loadVigente(conn, "sicoinprod") &&
						CGasto.fullGastoHistorico(conn,false, "sicoinprod") &&
						CGastoActual.LoadGastoActual(conn, "sicoinprod") &&
						CFechaActualizacionData.UpdateLoadDate("ejecucionpresupuestaria") &&
						CTrianguloNorte.loadEjesTrianguloNorte() &&
						CTrianguloNorte.loadEntidadesTrianguloNorte() &&
						CTrianguloNorte.loadEstructurasFinanciamiento() &&
						CFechaActualizacionData.UpdateLoadDate("paptn_ejecucionfinanciera") && 
						CEjecucionFisica.loadEjeucionHoja(conn, false, "sicoinprod") &&
						CEjecucionFisica.loadEjecucionDetalle(conn, false, "sicoinprod") &&
						CUnidadMedida.loadUnidadesMedida(conn, false, "sicoinprod")
					)
					CLogger.writeConsole("todas las tablas importadas con exito");
			 }
			 else if (cline.hasOption("update-all-des")){
				 int ejercicio = cline.getOptionValue("update_all_des")!=null ? Integer.parseInt(cline.getOptionValue("update_all_des")) : DateTime.now().getYear();
				 CLogger.writeConsole("Inicio de importacion de todos las tablas. SICOIN Descentralizado.");
				 CHive.close();
				 conn = CHive.openConnectiondes();
				 if(CAprobado.loadAprobados(conn,false, "sicoindescent") && 
						CAnticipo.loadAnticipos(conn,false, "sicoindescent") &&
						CEntidades.loadEntidades(conn,false, "sicoindescent") &&
						CEstructuras.loadEstructuras(conn, "sicoindescent", ejercicio) &&
						CFunciones.loadFunciones(conn, "sicoindescent", ejercicio) &&
						CFuentes.loadFuentes(conn, "sicoindescent") &&
						CGrupoGasto.loadGruposGasto(conn, "sicoindescent") &&
						CProyeccionGasto.calculateProyeccionGastoFuentesTributarias_EntidadMes(conn, "sicoindescent") &&
						CRenglones.loadRenglones(conn,false, "sicoindescent") &&
						CSubgrupoGasto.loadSubgruposGasto(conn, "sicoindescent") &&
						CUnidadesEjecutoras.loadUnidadesEjecutoras(conn, "sicoindescent") &&
						CVigente.loadVigente(conn, "sicoindescent") &&
						CGasto.fullGastoHistorico(conn,false, "sicoindescent") &&
						CGastoActual.LoadGastoActual(conn, "sicoindescent") && 
						CEjecucionFisica.loadEjeucionHoja(conn, false, "sicoindescent")  &&
						CEjecucionFisica.loadEjecucionDetalle(conn, false, "sicoindescent")  
					)
					CLogger.writeConsole("todas las tablas descentralizadas importadas con exito");
			 }
			 else if(cline.hasOption("help")){
				 HelpFormatter formater = new HelpFormatter();
				 formater.printHelp(80,"Utilitario para carga de informacion a MemSQL", "", options,"");
				 System.exit(0);
			 }
			 if(!cline.hasOption("help")){
				 DateTime now = new DateTime();
				 CLogger.writeConsole("Tiempo total: " + Minutes.minutesBetween(start, now).getMinutes() + " minutos " + (Seconds.secondsBetween(start, now).getSeconds() % 60) + " segundos " +
				 (now.getMillis()%1000) + " milisegundos ");
			 }
			 CHive.close(conn);
		 }
	 }
}
