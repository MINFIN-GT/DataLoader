package data_etl;

import java.lang.ProcessBuilder.Redirect;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.joda.time.DateTime;

import utilities.CLogger;

public class CGasto {
	public static boolean loadGastoPorEjercicioMesGeografico(Connection conn, boolean historico, String schema){
		DateTime date = new DateTime();
		PreparedStatement pstm;
		String query = "CREATE TABLE dashboard.gasto_ejercicio_mes_geografico_load AS select t2.geografico, t1.ejercicio, month(t1.fec_aprobado) mes, sum(t2.monto_renglon) total " + 
				"from "+schema+".eg_gastos_hoja t1, "+schema+".eg_gastos_detalle t2 " + 
				"where t1.ejercicio = t2.ejercicio  " + 
				"and t1.entidad = t2.entidad " + 
				"and t1.unidad_ejecutora = t2.unidad_ejecutora " + 
				"and t1.no_cur = t2.no_cur   " + 
				"and t1.clase_registro IN ('DEV', 'CYD', 'RDP', 'REG') " + 
				"and t1.estado = 'APROBADO' " + 
				((!historico) ? ("and t1.ejercicio = " + date.getYear()) : "") +" "+  
				"group by t2.geografico, t1.ejercicio, month(t1.fec_aprobado) ";
		boolean ret = false;
		try{
			if(!conn.isClosed()){
				pstm = conn.prepareStatement("DROP TABLE IF EXISTS dashboard.gasto_ejercicio_mes_geografico_load");
				pstm.executeUpdate();
				pstm.close();
				PreparedStatement pstm0 = conn.prepareStatement(query);
				pstm0.executeUpdate();
				boolean bconn=(schema.compareTo("sicoinprod")==0) ? CMemSQL.connect() : CMemSQL.connectdes();
				if(bconn){
					ret = true;
					int rows = 0;
					CLogger.writeConsole("CGasto:");
					
					pstm = conn.prepareStatement("SELECT count(*) FROM  dashboard.gasto_ejercicio_mes_geografico_load");
					ResultSet rs = pstm.executeQuery();
					rows=rs.next() ? rs.getInt(1) : 0;
					rs.close();
					if(rows>0) {
						PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("delete from gasto_ejercicio_mes_geografico "
								+ (!historico ? " where ejercicio="+date.getYear() : ""));
						if (pstm1.executeUpdate()>0)
							CLogger.writeConsole("Registros eliminados");
						else
							CLogger.writeConsole("Sin registros para eliminar");
						pstm1.close();
						String[] command = {"sh","-c","/usr/hdp/current/sqoop/bin/sqoop export -D mapred.job.queue.name=NodeMaster --connect jdbc:mysql://"+CMemSQL.getHost()+":"+CMemSQL.getPort()+"/"+CMemSQL.getSchema()+
								" --username "+CMemSQL.getUser()+" --table gasto_ejercicio_mes_geografico --hcatalog-database dashboard --hcatalog-table gasto_ejercicio_mes_geografico_load"};
						ProcessBuilder pb = new ProcessBuilder(command);
						pb.redirectOutput(Redirect.INHERIT);
						pb.redirectError(Redirect.INHERIT);
						pb.start().waitFor();
					}
					pstm.close();
					CLogger.writeConsole(String.join(" ","Total de records escritos: ",String.valueOf(rows)));
				}
			}
			pstm = conn.prepareStatement("DROP TABLE IF EXISTS dashboard.mv_ejecucion_presupuestaria_geografico_load");
			pstm.executeUpdate();
			pstm.close();
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 1: CGasto.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
	
	public static boolean fullGastoHistorico(Connection conn, boolean historico, String schema){
		DateTime date = new DateTime();
		PreparedStatement pstm;
		String query = "CREATE TABLE dashboard.eg_gasto_load AS select cast(ed.ejercicio as int) ejercicio, cast(month(eh.fec_aprobado) as int) mes, " + 
				" cast(eh.entidad as bigint) entidad, " +
				" cast(eh.unidad_ejecutora as int) unidad_ejecutora, "
				+ "cast(ed.programa as int) programa, cast(ed.subprograma as int) subprograma, "
				+ "cast(ed.proyecto as int) proyecto, cast(ed.actividad as int) actividad, cast(ed.obra as int) obra, "
				+ "cast(ed.organismo as int) organismo, cast(ed.correlativo as int) correlativo,  " +
				" cast(eh.no_cur as int) cur, cast(ed.geografico as int) geografico, cast(ed.fuente as int) fuente, " +
				" cast(ed.renglon as int) renglon,  cast(eh.clase_registro as int) clase_registro, "+
				"(case when eh.clase_registro='DEV' then 1 when eh.clase_registro='CYD' then 2 when eh.clase_registro='RDP' then 3 when eh.clase_registro='REG' then 4 else 5 end) iclase_registro, "+
				"ed.monto_renglon, cast(eh.estado as int) estado, "+
				"(case when eh.estado='APROBADO' then 1 else 0 end) iestado " + 
				"      from "+schema+".EG_GASTOS_DETALLE ed, "+schema+".EG_GASTOS_HOJA eh  " + 
				"      where ed.ejercicio = eh.EJERCICIO " + 
				"	   and month(eh.fec_aprobado) > 0" +
				"      and ed.ENTIDAD = eh.ENTIDAD " + 
				"      and ed.UNIDAD_EJECUTORA = eh.UNIDAD_EJECUTORA " + 
				"      and ed.NO_CUR = eh.no_cur " + 
				(!historico ? "	   and eh.ejercicio = " + date.getYear() + " " : "" );
		boolean ret = false;
		try{
			if(!conn.isClosed()){
				pstm = conn.prepareStatement("DROP TABLE IF EXISTS dashboard.eg_gasto_load");
				pstm.executeUpdate();
				pstm.close();
				PreparedStatement pstm0 = conn.prepareStatement(query);
				pstm0.executeUpdate();
				pstm0.close();
				boolean bconn=(schema.compareTo("sicoinprod")==0) ? CMemSQL.connect() : CMemSQL.connectdes();
				if(bconn){
					int rows = 0;
					CLogger.writeConsole("CGasto: Gasto por ejercicio, todo el detalle");
					
					pstm = conn.prepareStatement("SELECT count(*) FROM  dashboard.eg_gasto_load");
					ResultSet rs = pstm.executeQuery();
					rows=rs.next() ? rs.getInt(1) : 0;
					rs.close();
					if(rows>0) {
						PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("delete from eg_gasto "
								+ (!historico ? " where ejercicio="+date.getYear() : ""));
						if (pstm1.executeUpdate()>0)
							CLogger.writeConsole("Registros eliminados");
						else
							CLogger.writeConsole("Sin registros para eliminar");
						pstm1.close();
						String[] command = {"sh","-c","/usr/hdp/current/sqoop/bin/sqoop export -D mapred.job.queue.name=NodeMaster --connect jdbc:mysql://"+CMemSQL.getHost()+":"+CMemSQL.getPort()+"/"+CMemSQL.getSchema()+
								" --username "+CMemSQL.getUser()+" --table eg_gasto --hcatalog-database dashboard --hcatalog-table eg_gasto_load"};
						ProcessBuilder pb = new ProcessBuilder(command);
						pb.redirectOutput(Redirect.INHERIT);
						pb.redirectError(Redirect.INHERIT);
						pb.start().waitFor();
					}
					pstm.close();
					CLogger.writeConsole(String.join(" ","Total de records escritos: ",String.valueOf(rows)));
					ret = true;
				}
			}
			pstm = conn.prepareStatement("DROP TABLE IF EXISTS dashboard.eg_gasto_load");
			pstm.executeUpdate();
			pstm.close();
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 3: CGasto.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}
