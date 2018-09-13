package data_etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.joda.time.DateTime;

import utilities.CLogger;

public class CEntidades {
	
	public static boolean loadEntidades(Connection conn, boolean historico, String schema){
		DateTime date = new DateTime();
		String query = "SELECT e.ejercicio, e.entidad , e.nombre, " +
				" e.unidad_ejecutora, e.sigla, e.nit, e.codigo_departamento, e.codigo_municipio," +
				" SUM( CASE WHEN (ue.unidad_ejecutora > 0) THEN 1 ELSE 0 END) AS ues " +
				" FROM 	"+schema+".cg_entidades  e,  "+schema+".cg_entidades  ue " +
				" WHERE e.ejercicio = ue.ejercicio " +
				" AND e.entidad = ue.entidad " +
				" and e.restrictiva = 'N' " +
				" and ue.restrictiva = 'N' " +
				" and ue.ejecuta_gastos = 'S' "+
				(schema.compareTo("sicoinprod")==0  ? " and e.comportamiento='GOBIERNO_CENTRAL' "  : " and e.comportamiento='DESCENTRALIZADA'")+
				(!historico ? "	   and e.ejercicio = " + date.getYear() + " " : "" ) +
				" GROUP BY e.ejercicio, e.entidad , e.nombre , e.sigla, e.nit, e.codigo_departamento, e.codigo_municipio";
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
					CLogger.writeConsole("CEntidades:");
					PreparedStatement pstm;
					boolean first=true;
					pstm = CMemSQL.getConnection().prepareStatement("Insert INTO cg_entidades(ejercicio,entidad,nombre,sigla,nit,codigo_municipio,ues) "
							+ "values (?,?,?,?,?,?,?) ");
					while(rs.next()){
						if(first){
							PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("delete from cg_entidades "
									+ (!historico ? " where ejercicio=" + date.getYear() : ""))  ;
							if (pstm1.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");	
							pstm1.close();
							first=false;
						}
						pstm.setInt(1, rs.getInt("ejercicio"));
						pstm.setInt(2,rs.getInt("entidad"));
						pstm.setInt(2,rs.getInt("unidad_ejecutora"));
						pstm.setString(3, rs.getString("nombre"));
						pstm.setString(4, rs.getString("sigla"));
						pstm.setString(5, rs.getString("nit"));
						pstm.setInt(6,rs.getInt("codigo_municipio"));
						pstm.setInt(7,rs.getInt("ues"));
						pstm.addBatch();
						rows++;
						if((rows % 100) == 0){
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
