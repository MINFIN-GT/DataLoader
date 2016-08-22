package data_etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.joda.time.DateTime;

import utilities.CLogger;

public class CVigente {
	
	public static boolean loadVigente(Connection conn, String schema){
		DateTime date = new DateTime();
		String query = "select p.ejercicio,p.entidad, p.unidad_ejecutora, p.fuente, p.renglon, p.programa, p.subprograma, p.proyecto, p.actividad, p.obra, sum(p.asignado) asignado,(sum(p.asignado) + sum(p.adicion) + sum(p.disminucion) + sum(p.traspaso_p)+ sum(p.traspaso_n)+ sum(p.transferencia_p) + sum(p.transferencia_n)) vigente_actual, " + 
				"sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) vigente_enero, " + 
				"sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) + " + 
				"		   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) vigente_febrero, " + 
				"sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) + " + 
				"		   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) + " + 
				"		   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) vigente_marzo, " + 
				"sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) + " + 
				"		   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) + " + 
				"		   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) + " + 
				"		   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) vigente_abril, " + 
				"sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) + " + 
				"		   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) + " + 
				"		   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) + " + 
				"		   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) + " + 
				"	   	   (sum(p.adicion_05) + sum(p.disminucion_05) + sum(p.traspaso_p05)+ sum(p.traspaso_n05)+ sum(p.transferencia_p05) + sum(p.transferencia_n05) ) vigente_mayo, " + 
				"sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum("
				+ "p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) + " + 
				"		   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) + " + 
				"		   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) + " + 
				"		   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) + " + 
				"	   	   (sum(p.adicion_05) + sum(p.disminucion_05) + sum(p.traspaso_p05)+ sum(p.traspaso_n05)+ sum(p.transferencia_p05) + sum(p.transferencia_n05) ) + " + 
				"		   (sum(p.adicion_06) + sum(p.disminucion_06) + sum(p.traspaso_p06)+ sum(p.traspaso_n06)+ sum(p.transferencia_p06) + sum(p.transferencia_n06) ) vigente_junio, " + 
				"sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) + " + 
				"		   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) + " + 
				"		   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) + " + 
				"		   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) + " + 
				"	   	   (sum(p.adicion_05) + sum(p.disminucion_05) + sum(p.traspaso_p05)+ sum(p.traspaso_n05)+ sum(p.transferencia_p05) + sum(p.transferencia_n05) ) + " + 
				"		   (sum(p.adicion_06) + sum(p.disminucion_06) + sum(p.traspaso_p06)+ sum(p.traspaso_n06)+ sum(p.transferencia_p06) + sum(p.transferencia_n06) ) + " + 
				"		   (sum(p.adicion_07) + sum(p.disminucion_07) + sum(p.traspaso_p07)+ sum(p.traspaso_n07)+ sum(p.transferencia_p07) + sum(p.transferencia_n07) ) vigente_julio, " + 
				"sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) + " + 
				"		   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) + " + 
				"		   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) + " + 
				"		   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) + " + 
				"	   	   (sum(p.adicion_05) + sum(p.disminucion_05) + sum(p.traspaso_p05)+ sum(p.traspaso_n05)+ sum(p.transferencia_p05) + sum(p.transferencia_n05) ) + " + 
				"		   (sum(p.adicion_06) + sum(p.disminucion_06) + sum(p.traspaso_p06)+ sum(p.traspaso_n06)+ sum(p.transferencia_p06) + sum(p.transferencia_n06) ) + " + 
				"		   (sum(p.adicion_07) + sum(p.disminucion_07) + sum(p.traspaso_p07)+ sum(p.traspaso_n07)+ sum(p.transferencia_p07) + sum(p.transferencia_n07) ) + " + 
				"		   (sum(p.adicion_08) + sum(p.disminucion_08) + sum(p.traspaso_p08)+ sum(p.traspaso_n08)+ sum(p.transferencia_p08) + sum(p.transferencia_n08) ) vigente_agosto, " + 
				"sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) + " + 
				"		   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) + " + 
				"		   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) + " + 
				"		   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) + " + 
				"	   	   (sum(p.adicion_05) + sum(p.disminucion_05) + sum(p.traspaso_p05)+ sum(p.traspaso_n05)+ sum(p.transferencia_p05) + sum(p.transferencia_n05) ) + " + 
				"		   (sum(p.adicion_06) + sum(p.disminucion_06) + sum(p.traspaso_p06)+ sum(p.traspaso_n06)+ sum(p.transferencia_p06) + sum(p.transferencia_n06) ) + " + 
				"		   (sum(p.adicion_07) + sum(p.disminucion_07) + sum(p.traspaso_p07)+ sum(p.traspaso_n07)+ sum(p.transferencia_p07) + sum(p.transferencia_n07) ) + " + 
				"		   (sum(p.adicion_08) + sum(p.disminucion_08) + sum(p.traspaso_p08)+ sum(p.traspaso_n08)+ sum(p.transferencia_p08) + sum(p.transferencia_n08) ) + " + 
				"		   (sum(p.adicion_09) + sum(p.disminucion_09) + sum(p.traspaso_p09)+ sum(p.traspaso_n09)+ sum(p.transferencia_p09) + sum(p.transferencia_n09) ) vigente_septiembre, " + 
				"sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) + " + 
				"		   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) + " + 
				"		   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) + " + 
				"		   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) + " + 
				"	   	   (sum(p.adicion_05) + sum(p.disminucion_05) + sum(p.traspaso_p05)+ sum(p.traspaso_n05)+ sum(p.transferencia_p05) + sum(p.transferencia_n05) ) + " + 
				"		   (sum(p.adicion_06) + sum(p.disminucion_06) + sum(p.traspaso_p06)+ sum(p.traspaso_n06)+ sum(p.transferencia_p06) + sum(p.transferencia_n06) ) + " + 
				"		   (sum(p.adicion_07) + sum(p.disminucion_07) + sum(p.traspaso_p07)+ sum(p.traspaso_n07)+ sum(p.transferencia_p07) + sum(p.transferencia_n07) ) + " + 
				"		   (sum(p.adicion_08) + sum(p.disminucion_08) + sum(p.traspaso_p08)+ sum(p.traspaso_n08)+ sum(p.transferencia_p08) + sum(p.transferencia_n08) ) + " + 
				"		   (sum(p.adicion_09) + sum(p.disminucion_09) + sum(p.traspaso_p09)+ sum(p.traspaso_n09)+ sum(p.transferencia_p09) + sum(p.transferencia_n09) ) + " + 
				"		   (sum(p.adicion_10) + sum(p.disminucion_10) + sum(p.traspaso_p10)+ sum(p.traspaso_n10)+ sum(p.transferencia_p10) + sum(p.transferencia_n10) ) vigente_octubre, " + 
				"sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) + " + 
				"		   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) + " + 
				"		   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) + " + 
				"		   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) + " + 
				"	   	   (sum(p.adicion_05) + sum(p.disminucion_05) + sum(p.traspaso_p05)+ sum(p.traspaso_n05)+ sum(p.transferencia_p05) + sum(p.transferencia_n05) ) + " + 
				"		   (sum(p.adicion_06) + sum(p.disminucion_06) + sum(p.traspaso_p06)+ sum(p.traspaso_n06)+ sum(p.transferencia_p06) + sum(p.transferencia_n06) ) + " + 
				"		   (sum(p.adicion_07) + sum(p.disminucion_07) + sum(p.traspaso_p07)+ sum(p.traspaso_n07)+ sum(p.transferencia_p07) + sum(p.transferencia_n07) ) + " + 
				"		   (sum(p.adicion_08) + sum(p.disminucion_08) + sum(p.traspaso_p08)+ sum(p.traspaso_n08)+ sum(p.transferencia_p08) + sum(p.transferencia_n08) ) + " + 
				"		   (sum(p.adicion_09) + sum(p.disminucion_09) + sum(p.traspaso_p09)+ sum(p.traspaso_n09)+ sum(p.transferencia_p09) + sum(p.transferencia_n09) ) + " + 
				"		   (sum(p.adicion_10) + sum(p.disminucion_10) + sum(p.traspaso_p10)+ sum(p.traspaso_n10)+ sum(p.transferencia_p10) + sum(p.transferencia_n10) ) + " + 
				"		   (sum(p.adicion_11) + sum(p.disminucion_11) + sum(p.traspaso_p11)+ sum(p.traspaso_n11)+ sum(p.transferencia_p11) + sum(p.transferencia_n11) ) vigente_noviembre, " + 
				"sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) + " + 
				"		   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) + " + 
				"		   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) + " + 
				"		   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) + " + 
				"	   	   (sum(p.adicion_05) + sum(p.disminucion_05) + sum(p.traspaso_p05)+ sum(p.traspaso_n05)+ sum(p.transferencia_p05) + sum(p.transferencia_n05) ) + " + 
				"		   (sum(p.adicion_06) + sum(p.disminucion_06) + sum(p.traspaso_p06)+ sum(p.traspaso_n06)+ sum(p.transferencia_p06) + sum(p.transferencia_n06) ) + " + 
				"		   (sum(p.adicion_07) + sum(p.disminucion_07) + sum(p.traspaso_p07)+ sum(p.traspaso_n07)+ sum(p.transferencia_p07) + sum(p.transferencia_n07) ) + " + 
				"		   (sum(p.adicion_08) + sum(p.disminucion_08) + sum(p.traspaso_p08)+ sum(p.traspaso_n08)+ sum(p.transferencia_p08) + sum(p.transferencia_n08) ) + " + 
				"		   (sum(p.adicion_09) + sum(p.disminucion_09) + sum(p.traspaso_p09)+ sum(p.traspaso_n09)+ sum(p.transferencia_p09) + sum(p.transferencia_n09) ) + " + 
				"		   (sum(p.adicion_10) + sum(p.disminucion_10) + sum(p.traspaso_p10)+ sum(p.traspaso_n10)+ sum(p.transferencia_p10) + sum(p.transferencia_n10) ) + " + 
				"		   (sum(p.adicion_11) + sum(p.disminucion_11) + sum(p.traspaso_p11)+ sum(p.traspaso_n11)+ sum(p.transferencia_p11) + sum(p.transferencia_n11) ) + " + 
				"		   (sum(p.adicion_12) + sum(p.disminucion_12) + sum(p.traspaso_p12)+ sum(p.traspaso_n12)+ sum(p.transferencia_p12) + sum(p.transferencia_n12) ) vigente_diciembre " + 
				"from "+schema+".EG_F6_PARTIDAS p left join (select distinct p3.entidad from "+schema+".eg_f6_partidas p3 where p3.unidad_ejecutora > 0 and p3.ejercicio = year(current_date())) p2 on ( p2.entidad = p.entidad) " + 
				"where p.ejercicio= " + date.getYear() + 
				" and (p2.entidad is null or p.unidad_ejecutora>0) " + 
				" group by p.ejercicio,p.entidad,p.unidad_ejecutora,p.fuente,p.renglon,p.programa,p.subprograma, p.proyecto, p.actividad, p.obra";
		
		boolean ret = false;
		try{
			if(!conn.isClosed()){
				ResultSet rs = conn.prepareStatement(query).executeQuery();
				boolean bconn=(schema.compareTo("sicoinprod")==0) ? CMemSQL.connect() : CMemSQL.connectdes();
				if(rs!=null && bconn){
					ret = true;
					int rows = 0;
					CLogger.writeConsole("CVigente");
					PreparedStatement pstm;
					boolean first=true;
					pstm = CMemSQL.getConnection().prepareStatement("Insert INTO vigente(ejercicio,entidad,unidad_ejecutora,fuente,renglon,programa,subprograma,proyecto,actividad,obra, "
							+ "asignado, vigente_actual,vigente_1,vigente_2,vigente_3,vigente_4,vigente_5,vigente_6,vigente_7,vigente_8,vigente_9,vigente_10,vigente_11,vigente_12)"
							+ " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ");
					while(rs.next()){
						if(first){
							PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("delete from vigente ");
							if (pstm1.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");
							pstm1.close();
							first=false;
						}
						pstm.setInt(1,  rs.getInt("ejercicio"));
						pstm.setInt(2,  rs.getInt("entidad"));
						pstm.setInt(3,  rs.getInt("unidad_ejecutora"));
						pstm.setInt(4,  rs.getInt("fuente"));
						pstm.setInt(5,  rs.getInt("renglon"));
						pstm.setInt(6,  rs.getInt("programa"));
						pstm.setInt(7,  rs.getInt("subprograma"));
						pstm.setInt(8,  rs.getInt("proyecto"));
						pstm.setInt(9,  rs.getInt("actividad"));
						pstm.setInt(10,  rs.getInt("obra"));
						pstm.setDouble(11, rs.getDouble("asignado"));
						pstm.setDouble(12, rs.getDouble("vigente_actual"));						
						pstm.setDouble(13, rs.getDouble("vigente_enero"));
						pstm.setDouble(14, rs.getDouble("vigente_febrero"));
						pstm.setDouble(15, rs.getDouble("vigente_marzo"));
						pstm.setDouble(16, rs.getDouble("vigente_abril"));
						pstm.setDouble(17, rs.getDouble("vigente_mayo"));
						pstm.setDouble(18, rs.getDouble("vigente_junio"));
						pstm.setDouble(19, rs.getDouble("vigente_julio"));
						pstm.setDouble(20, rs.getDouble("vigente_agosto"));
						pstm.setDouble(21, rs.getDouble("vigente_septiembre"));
						pstm.setDouble(22, rs.getDouble("vigente_octubre"));
						pstm.setDouble(23, rs.getDouble("vigente_noviembre"));
						pstm.setDouble(24, rs.getDouble("vigente_diciembre"));
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
			CLogger.writeFullConsole("Error 1: CVigente.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}
