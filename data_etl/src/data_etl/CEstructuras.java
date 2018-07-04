package data_etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.joda.time.DateTime;

import utilities.CLogger;

public class CEstructuras {
	public static boolean loadEstructuras(Connection conn, String schema){
		DateTime date = new DateTime();
		String query = "select nivel_estructura, ejercicio, entidad, unidad_ejecutora, programa, subprograma, proyecto, obra, actividad,nom_estructura from "+schema+".cp_estructuras "
				+ "where ejercicio="+date.getYear();
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
					CLogger.writeConsole("CEstructuras");
					PreparedStatement pstm;
					
					boolean first=true;
					pstm = CMemSQL.getConnection().prepareStatement("Insert INTO cp_estructuras"
							+ "(nivel_estructura, ejercicio, entidad, unidad_ejecutora, programa, subprograma, proyecto, obra, actividad,nom_estructura) "
							+ "values (?,?,?,?,?,?,?,?,?,?) ");
					while(rs.next()){
						if(first){
							PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("delete from cp_estructuras "
									+ " where ejercicio="+date.getYear());
							if (pstm1.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");
							pstm1.close();
							first=false;
						}
						pstm.setInt(1, rs.getInt("nivel_estructura"));
						pstm.setInt(2,rs.getInt("ejercicio"));		
						pstm.setInt(3, rs.getInt("entidad"));
						pstm.setInt(4,rs.getInt("unidad_ejecutora"));		
						pstm.setInt(5, rs.getInt("programa"));
						pstm.setInt(6,rs.getInt("subprograma"));		
						pstm.setInt(7, rs.getInt("proyecto"));
						pstm.setInt(8,rs.getInt("obra"));		
						pstm.setInt(9, rs.getInt("actividad"));
						pstm.setString(10,rs.getString("nom_estructura"));
						pstm.addBatch();
						rows++;
						if((rows % 10000) == 0){
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
			CLogger.writeFullConsole("Error 1: CEstructuras.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}

}
