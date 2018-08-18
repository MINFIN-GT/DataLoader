package utilities;

import java.text.SimpleDateFormat;
import java.sql.Date;

public class CUtils {

	public static Date fromStringToDate(String date) {
		Date ret=null;
		try {
			ret=new Date(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.S").parse(date).getTime());
		} catch (Exception e) {
			ret=null;
		}  
		return ret;
	}
}
