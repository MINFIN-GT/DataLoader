package data_etl;

import java.lang.ProcessBuilder.Redirect;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.joda.time.DateTime;

import utilities.CLogger;

public class CGastoActual {
	public static boolean LoadGastoActual(Connection conn, String schema){
		DateTime date = new DateTime();
		PreparedStatement pstm;
		String query = "CREATE TABLE dashboard.eg_gasto_actual_load AS "
				+ "select cast(ed.ejercicio as int) ejercicio, cast(month(eh.fec_aprobado) as int) mes, "
				+ "cast(eh.entidad as bigint) entidad, cast(eh.unidad_ejecutora as int) unidad_ejecutora, "
				+ "cast(ed.programa as int) programa, cast(ed.subprograma as int) subprograma, "
				+ "cast(ed.proyecto as int) proyecto, cast( ed.actividad as int) actividad, "
				+ "cast(ed.obra as int) obra, cast(ed.organismo as int) organismo, "
				+ "cast(ed.correlativo as int) correlativo,  cast(eh.no_cur as int) cur, "
				+ "cast(ed.geografico as int) geografico, cast(ed.fuente as int), "+
				" 		cast(ed.renglon as int) renglon, cast(eh.clase_registro as int) clase_registro, "
				+ "(case when eh.clase_registro='DEV' then 1 when eh.clase_registro='CYD' then 2 when eh.clase_registro='RDP' then 3 when eh.clase_registro='REG' then 4 else 5 end) iclase_registro, "
				+ "ed.monto_renglon, cast(eh.estado as int) estado, " + 
				"(case when eh.estado='APROBADO' then 1 else 0 end) iestado " + 
				"      from "+schema+".EG_GASTOS_DETALLE ed, "+schema+".EG_GASTOS_HOJA eh  " + 
				"      where ed.ejercicio = eh.EJERCICIO " + 
				"      and ed.ENTIDAD = eh.ENTIDAD " + 
				"      and ed.UNIDAD_EJECUTORA = eh.UNIDAD_EJECUTORA " + 
				"      and ed.NO_CUR = eh.no_cur " + 
				"	   and eh.ejercicio = " + date.getYear();
		boolean ret = false;
		try{
			if(!conn.isClosed()){
				pstm = conn.prepareStatement("DROP TABLE IF EXISTS dashboard.eg_gasto_actual_load");
				pstm.executeUpdate();
				pstm.close();
				PreparedStatement pstm0 = conn.prepareStatement(query);
				pstm0.executeUpdate();
				boolean bconn=(schema.compareTo("sicoinprod")==0) ? CMemSQL.connect() : CMemSQL.connectdes();
				if(bconn){
					int rows = 0;
					CLogger.writeConsole("CGastoActual: Gasto del ejercicio actual, todo el detalle");
					
					pstm = conn.prepareStatement("SELECT count(*) FROM  dashboard.eg_gasto_actual_load");
					ResultSet rs = pstm.executeQuery();
					rows=rs.next() ? rs.getInt(1) : 0;
					rs.close();
					if(rows>0) {
						PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("delete from eg_gasto_actual_load ");
						if (pstm1.executeUpdate()>0)
							CLogger.writeConsole("Registros eliminados");
						else
							CLogger.writeConsole("Sin registros para eliminar");
						pstm1.close();
						String[] command = {"sh","-c","/usr/hdp/current/sqoop/bin/sqoop export -D mapred.job.queue.name=NodeMaster --connect jdbc:mysql://"+CMemSQL.getHost()+":"+CMemSQL.getPort()+"/"+CMemSQL.getSchema()+
								" --username "+CMemSQL.getUser()+" --table eg_gasto_actual --hcatalog-database dashboard --hcatalog-table eg_gasto_actual_load"};
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
			pstm = conn.prepareStatement("DROP TABLE IF EXISTS dashboard.eg_gasto_actual_load");
			pstm.executeUpdate();
			pstm.close();
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 1: CGastoActual.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}
