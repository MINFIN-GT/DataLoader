package data_etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import utilities.CLogger;
import utilities.CUtils;

public class CTesoreriaCuenta {
	public static boolean loadEstadoCuentas(Connection conn, String schema, int ejercicio){
		String query = "select * from "+schema+".te_estado_cuentas"
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
					CLogger.writeConsole("CTesoreriaCuenta carga de TE_ESTADO_CUENTAS");
					PreparedStatement pstm;
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
					pstm.executeBatch();
					CLogger.writeConsole(String.join(" ","Total de records escritos: ",String.valueOf(rows)));
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
}
