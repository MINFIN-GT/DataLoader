package data_etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.joda.time.DateTime;

import utilities.CLogger;

public class CGrupoGasto {
	public static boolean loadGruposGasto(Connection conn, String schema){
		DateTime date = new DateTime();
		String query = "select ejercicio, grupo_gasto, nombre, contenido from "+schema+".cp_grupos_gasto "
				+ "where restrictiva='N' and ejercicio="+date.getYear();
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
					CLogger.writeConsole("CGrupoGasto");
					PreparedStatement pstm;
					boolean first=true;
					pstm = CMemSQL.getConnection().prepareStatement("Insert INTO cp_grupos_gasto"
							+ "(ejercicio,grupo_gasto,nombre,contenido) "
							+ "values (?,?,?,?)");
					while(rs.next()){
						if(first){
							PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("delete from cp_grupos_gasto "
									+ "where ejercicio ="+date.getYear());
							if (pstm1.executeUpdate() > 0)
								CLogger.writeConsole("Eliminados los registros del ano en curso");
							else
								CLogger.writeConsole("Sin registros para eliminar");
							pstm1.close();
							first=false;
						}
						pstm.setInt(1, rs.getInt("ejercicio"));
						pstm.setInt(2,rs.getInt("grupo_gasto"));		
						pstm.setString(3, rs.getString("nombre"));
						pstm.setString(4, rs.getString("contenido"));	
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
			CLogger.writeFullConsole("Error 1: CGruposGastos.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}
