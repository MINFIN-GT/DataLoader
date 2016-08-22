package data_etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.joda.time.DateTime;

import utilities.CLogger;

public class CFuentes {
	public static boolean loadFuentes(Connection conn, String schema){
		DateTime date = new DateTime();
		String query = "select ejercicio, fuente, nombre, detalle, sigla from "+schema+".cg_fuentes "
				+ "where restrictiva='N' and ejercicio = "+date.getYear();
		boolean ret = false;
		try{
			if(!conn.isClosed()){
				ResultSet rs = conn.prepareStatement(query).executeQuery();
				boolean bconn=(schema.compareTo("sicoinprod")==0) ? CMemSQL.connect() : CMemSQL.connectdes();
				if(rs!=null && bconn){
					ret = true;
					int rows = 0;
					CLogger.writeConsole("CFuentes");
					PreparedStatement pstm;
					boolean first=true;
					pstm = CMemSQL.getConnection().prepareStatement("Insert INTO cg_fuentes"
							+ "(ejercicio,fuente,nombre,detalle,sigla) "
							+ "values (?,?,?,?,?) ");
					while(rs.next()){	
						if(first){
							PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("delete from cg_fuentes "
									+ " where ejercicio="+date.getYear());
							if (pstm1.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");
							pstm1.close();
							first=false;
						}
						pstm.setInt(1, rs.getInt("ejercicio"));
						pstm.setInt(2,rs.getInt("fuente"));		
						pstm.setString(3, rs.getString("nombre"));
						pstm.setString(4, rs.getString("detalle"));
						pstm.setString(5, rs.getString("sigla"));
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
			CLogger.writeFullConsole("Error 1: CFuentes.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}
