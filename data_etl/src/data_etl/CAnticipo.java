package data_etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.joda.time.DateTime;
import utilities.CLogger;

public class CAnticipo {
	public static boolean loadAnticipos(Connection conn, boolean historia, String schema){
		DateTime date = new DateTime();
		String query = "SELECT d.ejercicio, "+
       "d.entidad, "+
       "d.unidad_ejecutora, "+
       "d.fuente, "+
       "MONTH(h.FEC_APROBADO) MES_APROBACION, "+
       "d.cuatrimestre, "+
       "SUM( CASE "+
       "WHEN pMOD(d.mes,4)=1 THEN (d.cuota_fondo_rotativo_apr + d.cuota_fideicomisos_apr + d.CUOTA_CONVENIOS_APR + d.cuota_contratos_apr + d.cuota_otros_apr + d.cuota_paa_apr) ELSE 0 "+ 
       "END ) mes1_anticipo, "+
       "SUM( CASE "+
       "WHEN pMOD(d.mes,4)=2 THEN (d.cuota_fondo_rotativo_apr + d.cuota_fideicomisos_apr + d.CUOTA_CONVENIOS_APR + d.cuota_contratos_apr + d.cuota_otros_apr + d.cuota_paa_apr) ELSE 0 "+ 
	   "END ) mes2_anticipo, "+
	   "SUM( CASE "+
	   "  WHEN pMOD(d.mes,4)=3 THEN (d.cuota_fondo_rotativo_apr + d.cuota_fideicomisos_apr + d.CUOTA_CONVENIOS_APR + d.cuota_contratos_apr + d.cuota_otros_apr + d.cuota_paa_apr) ELSE 0 "+ 
	   "END ) mes3_anticipo, "+
	   "SUM( CASE "+
	   "  WHEN pMOD(d.mes,4)=0 THEN (d.cuota_fondo_rotativo_apr + d.cuota_fideicomisos_apr + d.CUOTA_CONVENIOS_APR + d.cuota_contratos_apr + d.cuota_otros_apr + d.cuota_paa_apr) ELSE 0 "+ 
	   "END ) mes4_anticipo "+
	   "FROM "+schema+".eg_anticipo_hoja h, "+
	   "     "+schema+".eg_anticipo_detalle d "+
	   "WHERE h.ejercicio = d.ejercicio "+
	  ((!historia) ? " and h.ejercicio="+date.getYear():"") +
	   " AND   h.entidad = d.entidad "+
	   "AND   h.unidad_ejecutora = d.unidad_ejecutora "+
	   "AND   h.no_cur = d.no_cur "+
	   "AND   h.estado = 'APROBADO' "+
	   "AND   h.clase_registro IN ('PRG','RPG') "+
	   "group by d.ejercicio, d.entidad, d.unidad_ejecutora, d.fuente, MONTH(h.FEC_APROBADO), d.cuatrimestre ";
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
					CLogger.writeConsole("CAnticipo:");
					PreparedStatement pstm;
					boolean first=true;
					pstm = CMemSQL.getConnection().prepareStatement("Insert INTO anticipo(ejercicio,entidad,unidad_ejecutora,mes_aprobacion,fuente,cuatrimestre, "
							+ "mes1_anticipo,mes2_anticipo,mes3_anticipo,mes4_anticipo)"
							+ " values (?,?,?,?,?,?,?,?,?,?)");
					while(rs.next()){
						if(first){
							PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("delete from anticipo "
									+ ((!historia) ? " where ejercicio="+date.getYear():""));
							if (pstm1.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");
							pstm1.close();
							first=false;
						}
						pstm.setInt(1, rs.getInt("ejercicio"));
						pstm.setInt(2,  rs.getInt("entidad"));
						pstm.setInt(3,  rs.getInt("unidad_ejecutora"));
						pstm.setInt(4,  rs.getInt("mes_aprobacion"));
						pstm.setInt(5,  rs.getInt("fuente"));
						pstm.setInt(6,  rs.getInt("cuatrimestre"));
						pstm.setDouble(7, rs.getDouble("mes1_anticipo"));
						pstm.setDouble(8, rs.getDouble("mes2_anticipo"));
						pstm.setDouble(9, rs.getDouble("mes3_anticipo"));
						pstm.setDouble(10, rs.getDouble("mes4_anticipo"));
						pstm.addBatch();
						if((rows % 1000) == 0)
							pstm.executeBatch();
						rows++;
						if((rows % 1000) == 0)
							CLogger.writeConsole(String.join(" ","Records escritos: ",String.valueOf(rows)));
					
					}
					pstm.executeBatch();
					CLogger.writeConsole(String.join(" ","Total de records escritos: ",String.valueOf(rows)));
					pstm.close();
				}
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 1: CAnticipo.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}

