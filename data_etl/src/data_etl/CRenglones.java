package data_etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.joda.time.DateTime;

import utilities.CLogger;

public class CRenglones {
	public static boolean loadRenglones(Connection conn,boolean historico, String schema){
		DateTime date = new DateTime();
		String query = "select distinct o.ejercicio, o.renglon, o.NOMBRE " + 
				"from "+schema+".cp_objetos_gasto o " +
				((!historico) ? " where o.ejercicio=  " + date.getYear() +" " : "");
		boolean ret = false;
		try{
			if(!conn.isClosed()){
				ResultSet rs = conn.prepareStatement(query).executeQuery();
				boolean bconn=(schema.compareTo("sicoinprod")==0) ? CMemSQL.connect() : CMemSQL.connectdes();
				if(rs!=null && bconn){
					ret = true;
					int rows = 0;
					CLogger.writeConsole("CRenglones");
					PreparedStatement pstm;
					boolean first=true;
					pstm = CMemSQL.getConnection().prepareStatement("Insert INTO renglones"
							+ "(ejercicio, entidad, unidad_ejecutora, renglon, subgrupo, grupo, nombre) "
							+ "values (?,?,?,?,?,?,?) ");
					while(rs.next()){
						if(first){
							PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("delete from renglones "
									+ (!historico ? " where ejercicio="+date.getYear() : ""));
							if (pstm1.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");
							pstm1.close();
							first=false;
						}
						int renglon = rs.getInt("renglon");
						int subgrupo = (renglon - ( renglon % 10));
						int grupo = subgrupo - (subgrupo % 100);
						pstm.setInt(1, rs.getInt("ejercicio"));
						pstm.setInt(2, 0);
						pstm.setInt(3, 0);
						pstm.setInt(4, renglon);
						pstm.setInt(5, subgrupo);
						pstm.setInt(6, grupo);
						pstm.setString(7, rs.getString("nombre"));
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
			CLogger.writeFullConsole("Error 1: CRenglones.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}
