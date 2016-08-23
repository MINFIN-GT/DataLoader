package data_etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.joda.time.DateTime;

import utilities.CLogger;

public class CUnidadesEjecutoras {
	
	public static boolean loadUnidadesEjecutoras(Connection conn, String schema){
		DateTime date = new DateTime();
		String query = "select ejercicio, entidad,unidad_ejecutora, nombre, sigla, nit, codigo_departamento, "
				+ "codigo_municipio from "+schema+".cg_entidades where "+(schema.compareTo("sicoinprod")==0 ? "comportamiento = 'GOBIERNO_CENTRAL' AND " : "comportamiento = 'DESCENTRALIZADA' AND ")
				+ "restrictiva = 'N' and ejercicio="+date.getYear();
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
					CLogger.writeConsole("CUnidadesEjecutoras");
					PreparedStatement pstm;
					boolean first=true;
					pstm = CMemSQL.getConnection().prepareStatement("Insert INTO unidades_ejecutoras"
							+ "(ejercicio,entidad,unidad_ejecutora,nombre,sigla,nit,codigo_municipio) "
							+ "values (?,?,?,?,?,?,?) ");
					while(rs.next()){	
						if(first){
							PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("delete from unidades_ejecutoras "
									+ " where ejercicio="+date.getYear());
							if (pstm1.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");
							pstm1.close();
							first=false;
						}
						pstm.setInt(1, rs.getInt("ejercicio"));
						pstm.setInt(2,rs.getInt("entidad"));		
						pstm.setInt(3, rs.getInt("unidad_ejecutora"));			
						pstm.setString(4, rs.getString("nombre"));
						pstm.setString(5, rs.getString("sigla"));
						pstm.setString(6, rs.getString("nit"));			
						pstm.setInt(7, rs.getInt("codigo_municipio"));
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
			CLogger.writeFullConsole("Error 1: CEntidades.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}
