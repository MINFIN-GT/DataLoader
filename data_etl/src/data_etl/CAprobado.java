package data_etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.joda.time.DateTime;
import utilities.CLogger;

public class CAprobado {
	public static boolean loadAprobados(Connection conn, boolean historia, String schema){
		DateTime date = new DateTime();
		String query = "SELECT d.ejercicio, "+
                   "d.entidad, "+
                   "d.unidad_ejecutora, "+
                   "d.unidad_ejecutora_origen, "+
                   "d.fuente,  "+
                   "d.grupo_gasto,  "+
				   "d.CUATRIMESTRE,  "+
                   "h1.clase_registro, "+ 
                   "month (H1.FEC_APROBADO) MES_APROBACION,  "+
                   "SUM (d.cuota_mes1_sol) mes1_sol, "+
                   "SUM (d.cuota_mes2_sol) mes2_sol, "+
                   "SUM (d.cuota_mes3_sol) mes3_sol, "+
                   "SUM (d.cuota_mes4_sol) mes4_sol, "+
                   "SUM (d.cuota_mes1_apr) mes1_apr, "+
                   "SUM (d.cuota_mes2_apr) mes2_apr, "+
                   "SUM (d.cuota_mes3_apr) mes3_apr, "+
                   "SUM (d.cuota_mes4_apr) mes4_apr "+
                   "FROM "+schema+".eg_financiero_detalle_4 D, "+
                   schema+".eg_financiero_hoja_4 H1 "+
                   "WHERE  h1.ejercicio = d.ejercicio "+
                   ((!historia) ? " and h1.ejercicio="+date.getYear():"") + " "+
                   " AND h1.entidad = d.entidad "+
                   " AND h1.unidad_ejecutora = d.unidad_ejecutora "+
                   " AND h1.unidad_desconcentrada = d.unidad_desconcentrada "+
                   " AND h1.no_cur = d.no_cur "+
                   " AND H1.CLASE_REGISTRO IN ('RPG', 'PRG', 'RPGI') "+
                   " AND H1.estado = 'APROBADO' "+
                   " GROUP BY d.ejercicio, "+
                   " d.entidad, "+
                   " d.unidad_ejecutora, "+
                   " d.unidad_ejecutora_origen, "+
                   " d.fuente, "+
                   " d.grupo_gasto, "+
                   " d.CUATRIMESTRE, "+
                   " h1.clase_registro, "+
                   " month (H1.FEC_APROBADO) ";
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
					CLogger.writeConsole("CAprobado:");
					PreparedStatement pstm;
					String clase_reg;
					boolean first=true;
					pstm = CMemSQL.getConnection().prepareStatement("Insert INTO aprobado(ejercicio,entidad,unidad_ejecutora,unidad_ejecutora_origen,FUENTE,grupo_gasto,"
							+ "clase_registro,iclase_registro,cuatrimestre,mes_aprobacion "
							+ ",mes1_sol,mes2_sol,mes3_sol,mes4_sol,mes1_apr,mes2_apr,mes3_apr,mes4_apr)"
							+ " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
					while(rs.next()){
						if(first){
							PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("delete from aprobado "
										+ ((!historia) ? " where ejercicio="+date.getYear():""));
							if (pstm1.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");
							pstm1.close();
							first=false;
						}
						pstm.setInt(1, rs.getInt("ejercicio"));
						pstm.setInt(2,  rs.getInt("entidad"));
						pstm.setInt(3,  rs.getInt("unidad_ejecutora"));
						pstm.setInt(4,  rs.getInt("unidad_ejecutora_origen"));
						pstm.setInt(5,  rs.getInt("fuente"));
						pstm.setInt(6,  rs.getInt("grupo_gasto"));						
						clase_reg = rs.getString("clase_registro");
						pstm.setString(7,  clase_reg);
						if ( clase_reg.compareTo("PRG")==0 )
							pstm.setInt(8, 1); 
						else if (clase_reg.compareTo("RPG")==0)
							pstm.setInt(8, 2); 
						else if (clase_reg.compareTo("RPGI")==0)
							pstm.setInt(8, 3); 
						else 
							pstm.setInt(8, 0); 												
						pstm.setInt(9,  rs.getInt("cuatrimestre"));
						pstm.setInt(10,  rs.getInt("mes_aprobacion"));	
						pstm.setDouble(11, rs.getDouble("mes1_sol"));
						pstm.setDouble(12, rs.getDouble("mes2_sol"));
						pstm.setDouble(13, rs.getDouble("mes3_sol"));
						pstm.setDouble(14, rs.getDouble("mes4_sol"));
						pstm.setDouble(15, rs.getDouble("mes1_apr"));
						pstm.setDouble(16, rs.getDouble("mes2_apr"));
						pstm.setDouble(17, rs.getDouble("mes3_apr"));
						pstm.setDouble(18, rs.getDouble("mes4_apr"));
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
			CLogger.writeFullConsole("Error 1: CAprobado.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}
