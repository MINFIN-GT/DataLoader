package data_etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.joda.time.DateTime;

import utilities.CLogger;

public class CUnidadMedida {
	public static boolean loadUnidadesMedida(Connection conn, boolean historico, String schema){
		DateTime date = new DateTime();
		String query = "SELECT clasificacion, codigo , ejercicio , grupo, nombre FROM "+schema+".fp_unidad_medida " +
				(!historico ? "	  WHERE ejercicio = " + date.getYear() + " " : "" );
		boolean ret = false;
		try{
			if(!conn.isClosed()){
				ResultSet rs = conn.prepareStatement(query).executeQuery();
				boolean bconn=(schema.compareTo("sicoinprod")==0) ? CMemSQL.connect() : CMemSQL.connectdes();
				if(rs!=null && bconn){
					ret = true;
					int rows = 0;
					CLogger.writeConsole("CUnidadMedida:");
					PreparedStatement pstm;
					boolean first=true;
					pstm = CMemSQL.getConnection().prepareStatement("Insert INTO fp_unidad_medida(clasificacion,codigo,ejercicio,grupo,nombre) "
							+ "values (?,?,?,?,?) ");
					while(rs.next()){
						if(first){
							PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("delete from fp_unidad_medida "
									+ (!historico ? " where ejercicio=" + date.getYear() : ""))  ;
							if (pstm1.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");	
							pstm1.close();
							first=false;
						}
						pstm.setInt(1, rs.getInt("clasificacion"));
						pstm.setInt(2,rs.getInt("codigo"));		
						pstm.setInt(3, rs.getInt("ejercicio"));
						pstm.setInt(4, rs.getInt("grupo"));
						pstm.setString(5, rs.getString("nombre"));
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
			CLogger.writeFullConsole("Error 1: CUnidadMedidad.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}
