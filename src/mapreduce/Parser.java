import java.util.*;


public class Parser {

	public static String EXAMPLE_SQL = "SELECT age FROM Users WHERE age > 20";

	public static ArrayList<String> getWhere(String query){
		int i = query.indexOf("WHERE");
		String temp = query.substring(i,query.length());	
		ArrayList<String> args = new ArrayList(Arrays.asList(temp.split(" ")));
		args = new ArrayList<String>(args.subList(1,4));
		return args;
	}

	public static String getTableName(String query){
		int i = query.indexOf("FROM");
		String temp = query.substring(i,query.length());
		ArrayList<String> args = new ArrayList(Arrays.asList(temp.split(" ")));
		return args.get(1);
	}
}
