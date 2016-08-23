package data_etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.joda.time.DateTime;

import utilities.CLogger;

public class CGasto {
	public static boolean loadGastoPorEjercicioMesGeografico(Connection conn, boolean historico, String schema){
		DateTime date = new DateTime();
		String query = "select t2.geografico, t1.ejercicio, month(t1.fec_aprobado) mes, sum(t2.monto_renglon) total " + 
				"from "+schema+".eg_gastos_hoja t1, "+schema+".eg_gastos_detalle t2 " + 
				"where t1.ejercicio = t2.ejercicio  " + 
				"and t1.entidad = t2.entidad " + 
				"and t1.unidad_ejecutora = t2.unidad_ejecutora " + 
				"and t1.no_cur = t2.no_cur   " + 
				"and t1.clase_registro IN ('DEV', 'CYD', 'RDP', 'REG') " + 
				"and t1.estado = 'APROBADO' " + 
				((!historico) ? ("and t1.ejercicio = " + date.getYear()) : "") +" "+  
				"group by t2.geografico, t1.ejercicio, month(t1.fec_aprobado) " + 
				"order by t2.geografico, t1.ejercicio, mes";
		boolean ret = false;
		try{
			if(!conn.isClosed()){
				PreparedStatement pstm0 = conn.prepareStatement(query);
				pstm0.setFetchSize(1000);
				ResultSet rs = pstm0.executeQuery();
				boolean bconn=(schema.compareTo("sicoinprod")==0) ? CMemSQL.connect() : CMemSQL.connectdes();
				if(rs!=null && bconn){
					ret = true;
					int rows = 0;
					CLogger.writeConsole("CGasto:");
					PreparedStatement pstm;
					boolean first=true;
					pstm = CMemSQL.getConnection().prepareStatement("Insert INTO gasto_ejercicio_mes_geografico(geografico,ejercicio,mes,total)"
							+ " values (?,?,?,?)");
					while(rs.next()){
						if(first){
							PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("delete from gasto_ejercicio_mes_geografico "
									+ (!historico ? " where ejercicio="+date.getYear() : ""));
							if (pstm1.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");
							pstm1.close();
							first=false;
						}
						pstm.setInt(1, rs.getInt("geografico"));
						pstm.setInt(2, rs.getInt("ejercicio"));
						pstm.setInt(3,  rs.getInt("mes"));
						pstm.setDouble(4, rs.getDouble("total"));
						pstm.addBatch();
						rows++;
						if((rows % 1000) == 0){
							pstm.executeBatch();
							CLogger.writeConsole(String.join(" ","Records escritos: ",String.valueOf(rows)));
						}
					}
					pstm.executeBatch();
					CLogger.writeConsole(String.join(" ","Total de records escritos: ",String.valueOf(rows)));
					pstm.close();
				}
			}
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
		
		String query = "select ed.ejercicio,month(eh.fec_aprobado) mes, eh.entidad, eh.unidad_ejecutora, ed.programa, ed.subprograma,ed.proyecto, ed.actividad,ed.obra,ed.organismo,ed.correlativo,  eh.no_cur, ed.geografico, ed.fuente, "+
				" 		ed.renglon, eh.clase_registro, ed.monto_renglon, eh.estado " + 
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
				ResultSet rs = conn.prepareStatement(query).executeQuery();
				boolean bconn=(schema.compareTo("sicoinprod")==0) ? CMemSQL.connect() : CMemSQL.connectdes();
				if(rs!=null && bconn){
					int rows = 0;
					CLogger.writeConsole("CGasto: Gasto por ejercicio, todo el detalle");
					PreparedStatement pstm;
					ret = true;
					int itemp=0;
					boolean first=true;
					pstm = CMemSQL.getConnection().prepareStatement("Insert INTO eg_gasto "
							+ "(ejercicio,mes,entidad,unidad_ejecutora,cur, organismo,correlativo,geografico, fuente, renglon,programa,subprograma,proyecto,actividad,obra, clase_registro, iclase_registro, monto_renglon, estado, iestado) "
							+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ");
					while(rs.next()){
						if(first){
							PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("delete from eg_gasto "
									+ (!historico ? " where ejercicio=" + date.getYear() : ""))  ;
							int rows_deleted=pstm1.executeUpdate();
							if (rows_deleted>0){
								CLogger.writeConsole("Registros eliminados");
							}
							else
								CLogger.writeConsole("Sin registros para eliminar");
							pstm1.close();
							first=false;
						}
						pstm.setInt(1, rs.getInt("ejercicio"));
						pstm.setInt(2, rs.getInt("mes"));
						pstm.setInt(3,rs.getInt("entidad"));
						pstm.setInt(4,rs.getInt("unidad_ejecutora"));
						pstm.setInt(5,rs.getInt("no_cur"));
						pstm.setInt(6,rs.getInt("organismo"));
						pstm.setInt(7,rs.getInt("correlativo"));
						pstm.setInt(8,rs.getInt("geografico"));
						pstm.setInt(9,rs.getInt("fuente"));
						pstm.setInt(10,rs.getInt("renglon"));
						pstm.setInt(11,rs.getInt("programa"));
						pstm.setInt(12,rs.getInt("subprograma"));
						pstm.setInt(13,rs.getInt("proyecto"));
						pstm.setInt(14,rs.getInt("actividad"));
						pstm.setInt(15,rs.getInt("obra"));
						pstm.setString(16,rs.getString("clase_registro"));
						itemp=0;
						if(rs.getString("clase_registro").compareTo("DEV")==0)
							itemp=1;
						else if(rs.getString("clase_registro").compareTo("CYD")==0)
							itemp=2;
						else if(rs.getString("clase_registro").compareTo("RDP")==0)
							itemp=3;
						else if(rs.getString("clase_registro").compareTo("REG")==0)
							itemp=4;
						pstm.setInt(17, itemp);
						pstm.setDouble(18,rs.getDouble("monto_renglon"));
						pstm.setString(19,rs.getString("estado"));
						itemp = (rs.getString("estado").compareTo("APROBADO")==0) ? 1 : 0;
						pstm.setInt(20, itemp);
						pstm.addBatch();
						rows++;
						if((rows % 1000) == 0)
							pstm.executeBatch();
						if(rows % 100000 == 0)
							CLogger.writeConsole(String.join(" ","Records escritos: ",String.valueOf(rows)));
					}
					pstm.executeBatch();
					CLogger.writeConsole(String.join(" ","Total de records escritos: ",String.valueOf(rows)));
					pstm.close();
				}
			}
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
