package data_etl;

import java.lang.ProcessBuilder.Redirect;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import utilities.CLogger;
import utilities.CUtils;

public class CTesoreriaCuenta {
	public static boolean loadEstadoCuentas(Connection conn, String schema, int ejercicio){
		boolean ret = false;
		try{
			if(!conn.isClosed()){
				boolean bconn=(schema.compareTo("sicoinprod")==0) ? CMemSQL.connect() : CMemSQL.connectdes();
				if(bconn){
					ret = true;
					CLogger.writeConsole("CTesoreriaCuenta carga de TE_ESTADO_CUENTAS");
					PreparedStatement pstm;
					pstm = conn.prepareStatement("DROP TABLE IF EXISTS sicoinprod.te_estado_cuentas_load");
					pstm.executeUpdate();
					pstm.close();
					pstm = conn.prepareStatement("CREATE TABLE sicoinprod.te_estado_cuentas_load AS SELECT cast(ejercicio as int) ejercicio, fec_banco, " + 
							"              cuenta_monetaria, cast(no_cheque_banco as bigint) no_cheque_banco, " + 
							"              cast(operacion_banco as bigint) operacion_banco, cast(error_hoja as bigint) error_hoja, monto_transaccion, " + 
							"              saldo_anterior, conciliado, fec_conciliado, fec_ingreso, origen_transaccion, restrictiva, tipo_conciliacion, fec_conciliacion_real, " +
							"			    no_relacion, tipo_cambio, cast(no_transaccion_lb as bigint) no_transaccion_lb, " + 
							"				fec_transaccion_lb, cast(operacion_banco_lb as bigint) operacion_banco_lb, cast(referencia_sicoin as bigint) referencia_sicoin, " +
							"				numero_lbtr, \"\" concepto, \"\" generado "+	
							" 				FROM sicoinprod.te_estado_cuentas WHERE ejercicio = ?");
					pstm.setInt(1, ejercicio);
					pstm.executeUpdate();
					pstm.close();
					pstm = conn.prepareStatement("SELECT count(*) total FROM  sicoinprod.te_estado_cuentas_load");
					ResultSet rs = pstm.executeQuery();
					int rows_total=rs.next() ? rs.getInt("total") : 0;
					rs.close();
					if(rows_total>0) {
						PreparedStatement pstm2 = CMemSQL.getConnection().prepareStatement("delete from te_estado_cuentas where ejercicio =  ? ");
						pstm2.setInt(1, ejercicio);
						if (pstm2.executeUpdate()>0)
							CLogger.writeConsole("Registros eliminados");
						else
							CLogger.writeConsole("Sin registros para eliminar");	
						pstm2.close();
						String[] command= {"sh","-c","/usr/hdp/current/sqoop/bin/sqoop export -D mapred.job.queue.name=NodeMaster --connect jdbc:mysql://"+CMemSQL.getHost()+":"+CMemSQL.getPort()+"/"+CMemSQL.getSchema()+
								" --username "+ CMemSQL.getUser()+ " --table te_estado_cuentas --hcatalog-database sicoinprod --hcatalog-table te_estado_cuentas_load"};
						ProcessBuilder pb = new ProcessBuilder(command);
						pb.redirectOutput(Redirect.INHERIT);
						pb.redirectError(Redirect.INHERIT);
						pb.start().waitFor();
					}
					pstm = conn.prepareStatement("DROP TABLE IF EXISTS sicoinprod.te_estado_cuentas_load");
					pstm.executeUpdate();
					pstm.close();
					/*PreparedStatement pstm;
					boolean first=true;
					pstm = CMemSQL.getConnection().prepareStatement("Insert INTO te_estado_cuentas"
							+ "(ejercicio,fec_banco,cuenta_monetaria,no_cheque_banco,operacion_banco, error_hoja,monto_transaccion,saldo_anterior,"
							+ "conciliado,fec_conciliado,fec_ingreso,origen_transaccion,restrictiva,tipo_conciliacion,fec_conciliacion_real,"
							+ "no_relacion,tipo_cambio,no_transaccion_lb,fec_transaccion_lb,operacion_banco_lb,referencia_sicoin,numero_lbtr,"
							+ "concepto,generado) "
							+ "values (?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?) ");
					while(rs.next()){
						if(first){
							PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("delete from te_estado_cuentas "
									+ " where ejercicio="+ejercicio);
							if (pstm1.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");
							pstm1.close();
							first=false;
						}
						pstm.setInt(1, rs.getInt("ejercicio"));
						pstm.setDate(2, CUtils.fromStringToDate(rs.getString("fec_banco")));
						pstm.setString(3, rs.getString("cuenta_monetaria"));
						pstm.setInt(4, rs.getInt("no_cheque_banco"));
						pstm.setInt(5, rs.getInt("operacion_banco"));
						pstm.setInt(6, rs.getInt("error_hoja"));
						pstm.setDouble(7, rs.getDouble("monto_transaccion"));
						pstm.setDouble(8, rs.getDouble("saldo_anterior"));
						pstm.setString(9, rs.getString("conciliado"));
						pstm.setDate(10, CUtils.fromStringToDate(rs.getString("fec_conciliado")));
						pstm.setDate(11, CUtils.fromStringToDate(rs.getString("fec_ingreso")));
						pstm.setString(12, rs.getString("origen_transaccion"));
						pstm.setString(13, rs.getString("restrictiva"));
						pstm.setString(14, rs.getString("tipo_conciliacion"));
						pstm.setDate(15, CUtils.fromStringToDate(rs.getString("fec_conciliacion_real")));
						pstm.setInt(16, rs.getInt("no_relacion"));
						pstm.setDouble(17, rs.getDouble("tipo_cambio"));
						pstm.setInt(18, rs.getInt("no_transaccion_lb"));
						pstm.setDate(19, CUtils.fromStringToDate(rs.getString("fec_transaccion_lb")));
						pstm.setInt(20, rs.getInt("operacion_banco_lb"));
						pstm.setInt(21, rs.getInt("referencia_sicoin"));
						pstm.setString(22, rs.getString("numero_lbtr"));
						pstm.setString(23, rs.getString("concepto"));
						pstm.setString(24, rs.getString("generado"));
						pstm.addBatch();
						rows++;
						if((rows % 1000) == 0){
							pstm.executeBatch();
							CLogger.writeConsole(String.join(" ","Records escritos: ",String.valueOf(rows)));
						}
					}
					pstm.executeBatch();*/
					CLogger.writeConsole(String.join(" ","Total de records escritos: ",String.valueOf(rows_total)));
					pstm.close();
					
				}
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 1: CTesoreriaCuenta.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
	
	public static boolean loadCuentas(Connection conn, String schema, int ejercicio){
		String query = "select * from "+schema+".te_cuentas_tesoreria"
				+ " where ejercicio="+ejercicio;
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
					CLogger.writeConsole("CTesoreriaCuenta carga de TE_CUENTAS_TESORERIA");
					PreparedStatement pstm;
					boolean first=true;
					pstm = CMemSQL.getConnection().prepareStatement("Insert INTO te_cuentas_tesoreria"
							+ "(ejercicio, cuenta_monetaria, nombre, banco,	nit, entidad, unidad_ejecutora, unidad_desconcentrada, cuenta_asociada,"
							+ "fuente, organismo, correlativo, saldo_inicial, saldo_actual, suma_debitos, suma_creditos, saldo_banco, concilia,"
							+ "sobregiro, chequera, pagadora, moneda, origen_cuenta, tipo_cuenta, observaciones, estado, fec_solicitada, fec_aprobada,"
							+ "fec_desactivada, restrictiva, fecha_eliminacion, saldo_anterior, principal, secundaria, cuenta_principal,"
							+ "reserva_acreedores, tramo, portafolio, flujo_caja, lbtr, cuenta_anterior) "
							+ "values (?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?,?,?,?,"
							+ "?) ");
					while(rs.next()){
						if(first){
							PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("delete from te_cuentas_tesoreria "
									+ " where ejercicio="+ejercicio);
							if (pstm1.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");
							pstm1.close();
							first=false;
						}
						pstm.setInt(1, rs.getInt("ejercicio"));
						pstm.setString(2, rs.getString("cuenta_monetaria"));
						pstm.setString(3, rs.getString("nombre"));
						pstm.setInt(4, rs.getInt("banco"));
						pstm.setString(5, rs.getString("nit"));
						pstm.setInt(6, rs.getInt("entidad"));
						pstm.setInt(7, rs.getInt("unidad_ejecutora"));
						pstm.setInt(8, rs.getInt("unidad_desconcentrada"));
						pstm.setString(9, rs.getString("cuenta_asociada"));
						pstm.setInt(10, rs.getInt("fuente"));
						pstm.setInt(11, rs.getInt("organismo"));
						pstm.setInt(12, rs.getInt("correlativo"));
						pstm.setDouble(13, rs.getDouble("saldo_inicial"));
						pstm.setDouble(14, rs.getDouble("saldo_actual"));
						pstm.setDouble(15, rs.getDouble("suma_debitos"));
						pstm.setDouble(16, rs.getDouble("suma_creditos"));
						pstm.setDouble(17, rs.getDouble("saldo_banco"));
						pstm.setString(18, rs.getString("concilia"));
						pstm.setString(19, rs.getString("sobregiro"));
						pstm.setString(20, rs.getString("chequera"));
						pstm.setString(21, rs.getString("pagadora"));
						pstm.setInt(22, rs.getInt("moneda"));
						pstm.setInt(23, rs.getInt("origen_cuenta"));
						pstm.setInt(24, rs.getInt("tipo_cuenta"));
						pstm.setString(25, rs.getString("observaciones"));
						pstm.setString(26, rs.getString("estado"));
						pstm.setDate(27, CUtils.fromStringToDate(rs.getString("fec_solicitada")));
						pstm.setDate(28, CUtils.fromStringToDate(rs.getString("fec_aprobada")));
						pstm.setDate(29, CUtils.fromStringToDate(rs.getString("fec_desactivada")));
						pstm.setString(30, rs.getString("restrictiva"));
						pstm.setDate(31, CUtils.fromStringToDate(rs.getString("fecha_eliminacion")));
						pstm.setDouble(32, rs.getDouble("saldo_anterior"));
						pstm.setString(33, rs.getString("principal"));
						pstm.setString(34, rs.getString("secundaria"));
						pstm.setString(35, rs.getString("cuenta_principal"));
						pstm.setDouble(36, rs.getDouble("reserva_acreedores"));
						pstm.setString(37, rs.getString("tramo"));
						pstm.setInt(38, rs.getInt("portafolio"));
						pstm.setString(39, rs.getString("flujo_caja"));
						pstm.setString(40, rs.getString("lbtr"));
						pstm.setString(41, rs.getString("cuenta_anterior"));
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
			CLogger.writeFullConsole("Error 2: CTesoreriaCuenta.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
	
	public static boolean loadTasasCambio(Connection conn, String schema, int ejercicio){
		String query = "select * from "+schema+".ct_tasas_cambio"
				+ " where year(fecha_inicio)="+ejercicio;
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
					CLogger.writeConsole("CTesoreriaCuenta carga de CT_TASAS_CAMBIO");
					PreparedStatement pstm;
					boolean first=true;
					pstm = CMemSQL.getConnection().prepareStatement("Insert INTO ct_tasas_cambio"
							+ "(organismo_define_tasa, fecha_inicio, fecha_fin, moneda, tasa_compra, tasa_venta, tasa_promedio,"
							+ "restrictiva, fecha_eliminacion) "
							+ "values (?,?,?,?,?,?,?,?,?) ");
					while(rs.next()){
						if(first){
							PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("delete from ct_tasas_cambio "
									+ " where year(fecha_inicio)="+ejercicio);
							if (pstm1.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");
							pstm1.close();
							first=false;
						}
						pstm.setInt(1, rs.getInt("organismo_define_tasa"));
						pstm.setDate(2, CUtils.fromStringToDate(rs.getString("fecha_inicio")));
						pstm.setDate(3, CUtils.fromStringToDate(rs.getString("fecha_fin")));
						pstm.setInt(4, rs.getInt("moneda"));
						pstm.setDouble(5, rs.getDouble("tasa_compra"));
						pstm.setDouble(6, rs.getDouble("tasa_venta"));
						pstm.setDouble(7, rs.getDouble("tasa_promedio"));
						pstm.setString(8, rs.getString("restrictiva"));
						pstm.setDate(9, CUtils.fromStringToDate(rs.getString("fecha_eliminacion")));
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
			CLogger.writeFullConsole("Error 3: CTesoreriaCuenta.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}
