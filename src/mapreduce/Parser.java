import java.util.*;


public class Parser {

	public static String EXAMPLE_SQL = "SELECT age, gender FROM Users WHERE age > 20";

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

	public static ArrayList<String> getSelectColumns(String query) {
		int i = query.indexOf("FROM");
		String temp = query.substring(7,i);
		ArrayList<String> args = new ArrayList(Arrays.asList(temp.split(", ")));
		ArrayList<String> ret = new ArrayList<String>();
		for(int j=0;j<args.size();j++){
			ret.add(args.get(j).trim());
		}
		return ret;
	}

	public static void main(String[] args){
		ArrayList<String> res = getSelectColumns(EXAMPLE_SQL);
		for(int i=0;i<res.size();i++){
			System.out.println(res.get(i));
		}
	}
}
