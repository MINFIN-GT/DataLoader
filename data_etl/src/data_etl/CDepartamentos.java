package data_etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import utilities.CLogger;

public class CDepartamentos {
	public static boolean loadDepartamentos(Connection conn){
		String query = "select * from sicoinprod.cg_departamentos where restrictiva='N'";
		boolean ret = false;
		try{
			if(!conn.isClosed()){
				ResultSet rs = conn.prepareStatement(query).executeQuery();
				if(rs!=null && CMemSQL.connect()){
					ret = true;
					int rows = 0;
					CLogger.writeConsole("CDepartamentos:");
					PreparedStatement pstm;
					boolean first=true;
					pstm = CMemSQL.getConnection().prepareStatement("Insert INTO departamento(codigo_departamento,nombre_departamento) values (?,?)");
					while(rs.next()){
						if(first){
							PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("delete from departamento");
							if (pstm1.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");
							pstm1.close();
							first=false;
						}
						pstm.setInt(1, rs.getInt("codigo_departamento"));
						pstm.setString(2,  rs.getString("nombre_departamento"));
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
			CLogger.writeFullConsole("Error 1: CDepartamentos.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}
