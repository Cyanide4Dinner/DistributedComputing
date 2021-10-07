import java.util.*;


public class Parser {

	public static String EXAMPLE_SQL = "SELECT occupation, gender, AVG(age) FROM Users WHERE age > 20 GROUP BY occupation, gender HAVING gender = M";

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

	public static ArrayList<String> getHaving(String query){
		int i = query.indexOf("HAVING");
		String temp = query.substring(i,query.length());
		ArrayList<String> args = new ArrayList(Arrays.asList(temp.split(" ")));
		args = new ArrayList<String>(args.subList(1,4));
		return args;
	}

	public static ArrayList<String> getGroupBy(String query){
		int i = query.indexOf("HAVING");
		String temp = query.substring(query.indexOf("GROUP BY")+8, i);
		ArrayList<String> args = new ArrayList(Arrays.asList(temp.split(", ")));
		ArrayList<String> ret = new ArrayList<String>();
		for(int j=0;j<args.size();j++){
			ret.add(args.get(j).trim());
		}
		return ret;
	}

	public static ArrayList<String> getAggregator(String query){
		ArrayList<String> ret = new ArrayList<String>();
		ArrayList<String> selectColumns = getSelectColumns(query);	
		ArrayList<String> having = getHaving(query);
		for(int i=0; i<selectColumns.size(); i++){
			String str = selectColumns.get(i);
			if(str.contains("COUNT")){
				ret.add("COUNT");
				ret.add(str.substring(str.indexOf("(")+1,str.indexOf(")")));
				return ret;
			}
			if(str.contains("MAX")){
				ret.add("MAX");
				ret.add(str.substring(str.indexOf("(")+1,str.indexOf(")")));
				return ret;
			}
			if(str.contains("MIN")){
				ret.add("MIN");
				ret.add(str.substring(str.indexOf("(")+1,str.indexOf(")")));
				return ret;
			}
			if(str.contains("AVG")){
				ret.add("AVG");
				ret.add(str.substring(str.indexOf("(")+1,str.indexOf(")")));
				return ret;
			}
			if(str.contains("SUM")){
				ret.add("SUM");
				ret.add(str.substring(str.indexOf("(")+1,str.indexOf(")")));
				return ret;
			}
		}

		for(int i=0; i<having.size(); i++){
			String str = having.get(i);
			if(str.contains("COUNT")){
				ret.add("COUNT");
				ret.add(str.substring(str.indexOf("(")+1,str.indexOf(")")));
				return ret;
			}
			if(str.contains("MAX")){
				ret.add("MAX");
				ret.add(str.substring(str.indexOf("(")+1,str.indexOf(")")));
				return ret;
			}
			if(str.contains("MIN")){
				ret.add("MIN");
				ret.add(str.substring(str.indexOf("(")+1,str.indexOf(")")));
				return ret;
			}
			if(str.contains("AVG")){
				ret.add("AVG");
				ret.add(str.substring(str.indexOf("(")+1,str.indexOf(")")));
				return ret;
			}
			if(str.contains("SUM")){
				ret.add("SUM");
				ret.add(str.substring(str.indexOf("(")+1,str.indexOf(")")));
				return ret;
			}
		}
		ret.add("");
		ret.add("");
		return ret;
	}

	public static void main(String[] args){
		ArrayList<String> res = getGroupBy(EXAMPLE_SQL);
		for(int i=0;i<res.size();i++){
			System.out.println("+"+res.get(i)+"+");
		}
	}
}
