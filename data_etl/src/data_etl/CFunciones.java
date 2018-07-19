package data_etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import utilities.CLogger;

public class CFunciones {
	public static boolean loadFunciones(Connection conn, String schema, int ejercicio){
		String query = "select ejercicio, funcion, nombre, contenido, sigla,hoja,restrictiva from "+schema+".cg_funciones "
				+ "where ejercicio = "+ejercicio;
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
					CLogger.writeConsole("CFunciones");
					PreparedStatement pstm;
					boolean first=true;
					pstm = CMemSQL.getConnection().prepareStatement("Insert INTO cg_funciones"
							+ "(ejercicio,funcion,nombre,contenido,sigla, hoja, restrictiva) "
							+ "values (?,?,?,?,?,?,?) ");
					while(rs.next()){	
						if(first){
							PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("delete from cg_funciones "
									+ " where ejercicio="+ejercicio);
							if (pstm1.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");
							pstm1.close();
							first=false;
						}
						pstm.setInt(1, rs.getInt("ejercicio"));
						pstm.setInt(2,rs.getInt("funcion"));		
						pstm.setString(3, rs.getString("nombre"));
						pstm.setString(4, rs.getString("contenido"));
						pstm.setString(5, rs.getString("sigla"));
						pstm.setString(6, rs.getString("hoja"));
						pstm.setString(7, rs.getString("restrictiva"));
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
			CLogger.writeFullConsole("Error 1: CFunciones.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}
