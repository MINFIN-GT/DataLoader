package data_etl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

import utilities.CLogger;

public class CMetas {
	
	static class st_meta_ep{
		int meta_presidencial_id;
		String meta_nombre;
		int entidad;
		String entidad_nombre;
		int unidad_ejecutora;
		String unidad_ejecutora_nombre;
		int programa;
		String programa_nombre;
		int subprograma;
		String subprograma_nombre;
		int proyecto;
		String proyecto_nombre;
		int actividad;
		String actividad_nombre;
		int obra;
		String obra_nombre;
		int renglon;
		String renlgon_nombre;
		double asignado;
		double vigente;
		double ejecutado;
	}
	
	static class st_renglon{
		int renglon;
		double asignado;
		double vigente;
		String nombre;
	}
	
	public static boolean loadMetas(int mes){
		boolean ret = false;
		try{
			if( CMemSQL.connect() && CMemSQL.connectdes()){
				ret = true;

				CLogger.writeConsole("CMetas Entidades:");
				PreparedStatement pstm = CMemSQL.getConnection().prepareStatement("DELETE FROM mv_meta WHERE ejercicio = year(CURRENT_TIMESTAMP) ");
				if (pstm.executeUpdate()>0)
					CLogger.writeConsole("Registros eliminados");
				else
					CLogger.writeConsole("Sin registros para eliminar");
				pstm.close();
				pstm = CMemSQL.getConnection().prepareStatement("SELECT distinct mp.id, mp.nombre, ep.entidad, e.nombre entidad_nombre, " + 
						"ep.unidad_ejecutora,  ues.nombre unidad_ejecutora_nombre," + 
						"ep.programa, p.nom_estructura programa_nombre, " + 
						"ep.subprograma, sp.nom_estructura subprograma_nombre," + 
						"ep.proyecto, pr.nom_estructura proyecto_nombre, " + 
						"ep.actividad, ac.nom_estructura actividad_nombre, " + 
						"ep.obra " + 
						"FROM meta_presidencial mp, meta_presidencial_ep ep, " + 
						"cg_entidades e, unidades_ejecutoras ues, cp_estructuras p, cp_estructuras sp, cp_estructuras pr, cp_estructuras ac " + 
						"WHERE mp.id = ep.meta_presidencialid " + 
						"and ep.entidad = e.entidad  " + 
						"and (ues.entidad = ep.entidad and ues.unidad_ejecutora = ep.unidad_ejecutora) " + 
						"and (p.entidad = ep.entidad and p.unidad_ejecutora = ep.unidad_ejecutora and p.programa = ep.programa and p.nivel_estructura = 2) " + 
						"and (sp.entidad = ep.entidad and sp.unidad_ejecutora = ep.unidad_ejecutora and sp.programa = ep.programa and sp.subprograma = ep.subprograma and sp.nivel_estructura = 3) " + 
						"and (pr.entidad = ep.entidad and pr.unidad_ejecutora = ep.unidad_ejecutora and pr.programa = ep.programa and pr.subprograma = ep.subprograma and pr.proyecto = ep.proyecto and pr.nivel_estructura = 4) " + 
						"and (ac.entidad = ep.entidad and ac.unidad_ejecutora = ep.unidad_ejecutora and ac.programa = ep.programa and ac.subprograma = ep.subprograma and ac.proyecto = ep.proyecto and ac.actividad = ep.actividad and ac.nivel_estructura = 5) " + 
						"AND ep.ejercicio = year(CURRENT_TIMESTAMP) " + 
						"AND e.ejercicio = ep.ejercicio " + 
						"and ues.ejercicio = ep.ejercicio " + 
						"and p.ejercicio = ep.ejercicio " + 
						"and sp.ejercicio = ep.ejercicio " + 
						"and pr.ejercicio = ep.ejercicio " + 
						"and ac.ejercicio = ep.ejercicio " + 
						"and ep.tipo_entidad=1 "+ //Centralizadas
						"order by mp.id, mp.nombre, ep.programa, ep.subprograma, ep.proyecto, ep.actividad, ep.obra");  
				ResultSet rs = pstm.executeQuery();
				ArrayList<st_meta_ep> metas = new ArrayList<st_meta_ep>();
				while(rs.next()){
					ArrayList<st_renglon> renglones = CMetas.getRenglonesConVigenteAprobado(mes, rs.getInt("entidad"), rs.getInt("unidad_ejecutora"), 
							rs.getInt("programa"), rs.getInt("subprograma"), rs.getInt("proyecto"),rs.getInt("actividad"), rs.getInt("obra"));
					for(st_renglon renglon : renglones){
						st_meta_ep temp = new CMetas.st_meta_ep();
						temp.actividad = rs.getInt("actividad");
						temp.actividad_nombre = rs.getString("actividad_nombre");
						temp.asignado = renglon.asignado;
						temp.entidad = rs.getInt("entidad");
						temp.entidad_nombre = rs.getString("entidad_nombre");
						temp.meta_nombre = rs.getString("mp.nombre");
						temp.meta_presidencial_id = rs.getInt("mp.id");
						temp.obra = rs.getInt("obra");
						temp.obra_nombre = rs.getString("obra_nombre");
						temp.programa =  rs.getInt("programa");
						temp.programa_nombre = rs.getString("programa_nombre");
						temp.proyecto = rs.getInt("proyecto");
						temp.proyecto_nombre = rs.getString("proyecto_nombre");
						temp.renglon = renglon.renglon;
						temp.renlgon_nombre = renglon.nombre;
						temp.subprograma = rs.getInt("subprograma");
						temp.subprograma_nombre = rs.getString("subprograma_nombre");
						temp.unidad_ejecutora = rs.getInt("unidad_ejecutora");
						temp.unidad_ejecutora_nombre = rs.getString("unidad_ejecutora_nombre");
						temp.vigente =  renglon.vigente;
						temp.ejecutado = CMetas.getEjecutado(mes, temp.entidad, temp.unidad_ejecutora, temp.programa, temp.subprograma, temp.proyecto, 
								temp.actividad, temp.obra, temp.renglon);
						metas.add(temp);
					}
				} 
				pstm.close();
			}					
				
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 1: CMetas.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
	
	private static Double getEjecutado(int mes,int entidad, int unidad_ejecutora, int programa, int subprograma, int proyecto, int actividad, int obra, int renglon){
		Double ret=0.0;
		try{
			if( CMemSQL.connect()){
				PreparedStatement pstm = CMemSQL.getConnection().prepareStatement("SELECT SUM(total) total "
						+ "FROM vw_gasto_renglon_programa WHERE  entidad=? and ue=? and programa=? and subprograma=? and proyecto=? and actividad=? and obra=? and renlgon=? and mes<=?");
				pstm.setInt(1, entidad);
				pstm.setInt(2, unidad_ejecutora);
				pstm.setInt(3, programa);
				pstm.setInt(4, subprograma);
				pstm.setInt(5, proyecto);
				pstm.setInt(6, actividad);
				pstm.setInt(7, obra);
				pstm.setInt(8, renglon);
				pstm.setInt(9, mes);
				ResultSet rs = pstm.executeQuery();
				if(rs.next()){
					ret = rs.getDouble(1);
				}
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 2: CMetas.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
	
	private static ArrayList<st_renglon> getRenglonesConVigenteAprobado(int mes,int entidad, int unidad_ejecutora, int programa, int subprograma, int proyecto, int actividad, int obra){
		ArrayList<st_renglon> ret= new ArrayList<st_renglon>();
		try{
			if( CMemSQL.connect()){
				PreparedStatement pstm = CMemSQL.getConnection().prepareStatement("SELECT v.renglon, v.vigente_"+mes+", v.asignado, r.nombre "
						+ "FROM vw_vigente_renglon_programa v, renglones r WHERE  v.entidad=? and v.ue=? and v.programa=? and v.subprograma=? and v.proyecto=? and v.actividad=? and v.obra=? "
						+ "and (v.vigente_"+mes+">0 OR v.asignado>0) and v.renglon = r.renglon");
				pstm.setInt(1, entidad);
				pstm.setInt(2, unidad_ejecutora);
				pstm.setInt(3, programa);
				pstm.setInt(4, subprograma);
				pstm.setInt(5, proyecto);
				pstm.setInt(6, actividad);
				pstm.setInt(7, obra);
				ResultSet rs = pstm.executeQuery();
				while(rs.next()){
					st_renglon temp=new CMetas.st_renglon();
					temp.renglon = rs.getInt("renglon");
					temp.asignado = rs.getDouble("asignado");
					temp.vigente = rs.getDouble("vigente_"+mes);
					temp.nombre = rs.getString("nombre");
					ret.add(temp);
				}
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 4: CMetas.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}
