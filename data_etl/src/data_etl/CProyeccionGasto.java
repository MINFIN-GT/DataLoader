package data_etl;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.joda.time.DateTime;

import utilities.CLogger;

public class CProyeccionGasto {
	
	public static boolean calculateProyeccionGastoFuentesTributarias_EntidadMes(Connection conn, String schema){
		DateTime date = new DateTime();
		
		String query = "select entidad, mes, " + 
				"case when sum(total)>0  then (sum(mensual)/sum(total)) " + 
				"else 0.00 end indice " + 
				"from ( " + 
				"    select mensual.ejercicio, mensual.entidad, mensual.unidad_ejecutora, mensual.renglon, mensual.fuente, mensual.mes, mensual.mensual, anual.total " + 
				"    from ( " + 
				"      select ed.ejercicio, eh.entidad, eh.unidad_ejecutora, ed.renglon, ed.fuente, month(eh.fec_aprobado) mes, sum(ed.monto_renglon) mensual " + 
				"      from "+schema+".EG_GASTOS_DETALLE ed, "+schema+".EG_GASTOS_HOJA eh  " + 
				"      where ed.ejercicio = eh.EJERCICIO " + 
				"      and ed.ENTIDAD = eh.ENTIDAD " + 
				"      and ed.UNIDAD_EJECUTORA = eh.UNIDAD_EJECUTORA " + 
				"	   and ed.fuente IN (11,12,13,14,15,16,21,22,29) "+
				"      and ed.NO_CUR = eh.no_cur " + 
				"      and eh.CLASE_REGISTRO IN ('DEV', 'CYD', 'RDP', 'REG') " + 
				"      and eh.ESTADO = 'APROBADO' " + 
				"      group by ed.ejercicio, eh.entidad, eh.unidad_ejecutora, ed.renglon, ed.fuente, month(eh.fec_aprobado) " + 
				"    ) mensual, " + 
				"    ( " + 
				"      select eh1.entidad, eh1.unidad_ejecutora, ed1.renglon, ed1.fuente, sum(ed1.monto_renglon) total " + 
				"    from "+schema+".EG_GASTOS_DETALLE ed1, "+schema+".EG_GASTOS_HOJA eh1  " + 
				"    where ed1.ejercicio = eh1.EJERCICIO " + 
				"    and ed1.ENTIDAD = eh1.ENTIDAD " + 
				"    and ed1.UNIDAD_EJECUTORA = eh1.UNIDAD_EJECUTORA " +
				"	 and ed1.fuente IN (11,12,13,14,15,16,21,22,29) "+
				"    and ed1.NO_CUR = eh1.no_cur " + 
				"    and eh1.CLASE_REGISTRO IN ('DEV', 'CYD', 'RDP', 'REG') " + 
				"    and eh1.ESTADO = 'APROBADO' " + 
				"    group by eh1.entidad, eh1.unidad_ejecutora, ed1.renglon, ed1.fuente " + 
				"    ) anual " + 
				"    where mensual.entidad = anual.entidad " + 
				"    and mensual.unidad_ejecutora = anual.unidad_ejecutora " + 
				"    and mensual.renglon = anual.renglon " + 
				"    and mensual.fuente = anual.fuente " + 
				"    order by mensual.entidad, mensual.unidad_ejecutora, mensual.renglon, mensual.fuente, mensual.mes, mensual.mensual, mensual.ejercicio " + 
				") todo " + 
				"where ejercicio < " + date.getYear() + " " + 
				"group by entidad,  mes";
		boolean ret = false;
		try{
			if(!conn.isClosed()){
				PreparedStatement pstm0 = conn.prepareStatement(query);
				pstm0.setFetchSize(1000);
				ResultSet rs = pstm0.executeQuery();
				boolean bconn=(schema.compareTo("sicoinprod")==0) ? CMemSQL.connect() : CMemSQL.connectdes();
				if(rs!=null && bconn){
					int rows = 0;
					CLogger.writeConsole("CProyeccionGasto: Indicies de la proyeccion del gasto por entidad y mes");
					PreparedStatement pstm;
					ret = true;
					boolean first=true;
					pstm = CMemSQL.getConnection().prepareStatement("Insert INTO pgi_ft_entidad_mes"
							+ "(entidad,mes,indice) "
							+ "values (?,?,?) "
							+ "ON DUPLICATE KEY UPDATE INDICE = ?");
					while(rs.next()){
						if(first){
							PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("delete from pgi_ft_entidad_mes ");
							if (pstm1.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");
							pstm1.close();
							first=false;
						}
						pstm.setInt(1, rs.getInt("entidad"));
						pstm.setInt(2,rs.getInt("mes"));		
						pstm.setBigDecimal(3, new BigDecimal( rs.getString("indice")));		
						pstm.setBigDecimal(4, new BigDecimal( rs.getString("indice")));	
						pstm.addBatch();
						rows++;
						if(rows % 1000 == 0){
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
			CLogger.writeFullConsole("Error 1: CProyeccionGasto.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
	
	
}
