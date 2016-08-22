package data_etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.joda.time.DateTime;

import utilities.CLogger;

public class CGeograficos {
	public static boolean loadGeograficos(Connection conn, String schema){
		DateTime date = new DateTime();
		String query = "select ejercicio, geografico, nombre, region from "+schema+".cg_geograficos "
				+ "where restrictiva='N' and ejercicio="+date.getYear();
		boolean ret = false;
		try{
			if(!conn.isClosed()){
				ResultSet rs = conn.prepareStatement(query).executeQuery();
				boolean bconn=(schema.compareTo("sicoinprod")==0) ? CMemSQL.connect() : CMemSQL.connectdes();
				if(rs!=null && bconn){
					ret = true;
					int rows = 0;
					CLogger.writeConsole("CGeograficos");
					PreparedStatement pstm;
					boolean first=true;
					pstm = CMemSQL.getConnection().prepareStatement("Insert INTO cg_geograficos"
							+ "(ejercicio,geografico,nombre,region) "
							+ "values (?,?,?,?) ");
					while(rs.next()){
						if(first){
							PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("delete from cg_geograficos "
									+ " where ejercicio="+date.getYear());
							if (pstm1.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");
							pstm1.close();
							first=false;
						}
						pstm.setInt(1, rs.getInt("ejercicio"));
						pstm.setInt(2,rs.getInt("geografico"));		
						pstm.setString(3, rs.getString("nombre"));
						pstm.setInt(4, rs.getInt("region"));
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
			CLogger.writeFullConsole("Error 1: CGeograficos.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}
