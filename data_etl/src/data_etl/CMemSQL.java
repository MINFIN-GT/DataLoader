package data_etl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import utilities.CLogger;
import utilities.CProperties;


public class CMemSQL {
	private static Connection connection;
	private static String host;
	private static Integer port; 
	private static String user;
	private static String password;
	private static String schema;
	private static String schemades;
	
	private static Statement st;
	
	static {		
		host = CProperties.getmemsql_host();
		port = CProperties.getmemsql_port();
		user = CProperties.getmemsql_user();
		password = CProperties.getmemsql_password();
		schema = CProperties.getmemsql_schema();
		schemades = CProperties.getmemsql_schemades();
	}
	
	public static String getHost() {
		return host;
	}

	public static String getUser() {
		return user;
	}

	public static void setUser(String user) {
		CMemSQL.user = user;
	}

	public static void setHost(String host) {
		CMemSQL.host = host;
	}

	public static Integer getPort() {
		return port;
	}

	public static void setPort(Integer port) {
		CMemSQL.port = port;
	}

	public static String getPassword() {
		return password;
	}

	public static void setPassword(String password) {
		CMemSQL.password = password;
	}

	public static String getSchema() {
		return schema;
	}

	public static void setSchema(String schema) {
		CMemSQL.schema = schema;
	}

	public static String getSchemades() {
		return schemades;
	}

	public static void setSchemades(String schemades) {
		CMemSQL.schemades = schemades;
	}

	public static boolean connect(){
		try{
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			connection=DriverManager.getConnection("jdbc:mysql://"+host+":"+port+"/"+schema+"?" +
                    "user="+user+"&password="+password);
			if(!connection.isClosed())
				return true;
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 1 : CMemSQL.class ", e);
		}
		return false;
	}
	
	public static boolean connectdes(){
		try{
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			connection=DriverManager.getConnection("jdbc:mysql://"+host+":"+port+"/"+schemades+"?" +
                    "user="+user+"&password="+password);
			if(!connection.isClosed())
				return true;
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 2 : CMemSQL.class ", e);
		}
		return false;
	}
	
	public static Connection getConnection(){
		return connection;
	}
	

	public static void close(){
		try {
			connection.close();
		} catch (SQLException e) {
			CLogger.writeFullConsole("Error 3 : CMemSQL.class ", e);
		}
	}
	
	public static long getNextID(Connection connection,String table){
		long ret = -1;
		try{
			PreparedStatement stm = connection.prepareStatement("SELECT last_id FROM uid WHERE table_name=?");
			stm.setString(1, table);
			ResultSet rs = stm.executeQuery();
			if(rs.next()){
				ret = rs.getLong("last_id");
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 4: CMemSQL.class", e);
		}
		return ret;
	}
	
	
	public static boolean saveLastID(Connection connection, String table, long last_id){
		boolean ret = false;
		try{
			PreparedStatement stm = connection.prepareStatement("UPDATE uid SET last_id=? WHERE table_name=?");
			stm.setLong(1, last_id);
			stm.setString(2, table);
			if(stm.executeUpdate()>0)
				ret = true;
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 5: CMemSQL.class", e);
		}
		return ret;
	}
	
	public static ResultSet runQuery(String query){
		ResultSet ret=null;
		try{
			if(!connection.isClosed()){
				st = connection.createStatement();
				ret = st.executeQuery(query);
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 6: CMemSQL.class", e);
		}
		return ret;
	}
}
