package data_etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.joda.time.DateTime;

import utilities.CLogger;

public class CSubgrupoGasto {
	public static boolean loadSubgruposGasto(Connection conn, String schema){
		DateTime date = new DateTime();
		String query = "select ejercicio, renglon, nombre, grupo_gasto, contenido from "+schema+".cp_objetos_gasto "
				+ "where renglon>0 and pmod(renglon,100) > 0 and pmod(renglon,10)==0 and ejercicio = "+date.getYear();
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
					CLogger.writeConsole("CSubgrupoGasto:");
					PreparedStatement pstm;
					boolean first=true;
					pstm = CMemSQL.getConnection().prepareStatement("Insert INTO subgrupo_gasto_ejercicio"
							+ "(ejercicio,grupo_gasto,sub_grupo_gasto,nombre,contenido) "
							+ "values (?,?,?,?,?) ");
					while(rs.next()){	
						if(first){
							PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("delete from subgrupo_gasto_ejercicio "
									+ " where ejercicio="+date.getYear());
							if (pstm1.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");
							pstm1.close();
							first=false;
						}
						pstm.setInt(1, rs.getInt("ejercicio"));
						pstm.setInt(2,rs.getInt("grupo_gasto"));		
						pstm.setInt(3, rs.getInt("renglon"));			
						pstm.setString(4, rs.getString("nombre"));
						pstm.setString(5, rs.getString("contenido"));		
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
			CLogger.writeFullConsole("Error 1: CSubgruposGastos.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}
