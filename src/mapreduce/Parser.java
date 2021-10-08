import java.util.*;


public class Parser {

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

	public static String getJoinTableName(String query){
		int i = query.indexOf("JOIN");
		String temp = query.substring(i,query.length());
		ArrayList<String> args = new ArrayList(Arrays.asList(temp.split(" ")));
		return args.get(1);
	}

	public static String getJoinColumn(String query){
		int i = query.indexOf("ON");
		String temp = query.substring(i, query.length());
		ArrayList<String> args = new ArrayList(Arrays.asList(temp.split(" ")));
		return args.get(1);
	}

	public static ArrayList<String> getJoinColumnTable1(String query){
		int i = query.indexOf("ON");
		String temp = query.substring(i, query.length());
		ArrayList<String> args = new ArrayList(Arrays.asList(temp.split(" ")));
		ArrayList<String> ret = new ArrayList<String>();
		ret.add(args.get(1).substring(0,args.get(1).trim().indexOf(".")));
		ret.add(args.get(1).substring(args.get(1).trim().indexOf(".")+1,args.get(1).trim().length()));
		return ret;
	}

	public static ArrayList<String> getJoinColumnTable2(String query){
		int i = query.indexOf("ON");
		String temp = query.substring(i, query.length());
		ArrayList<String> args = new ArrayList(Arrays.asList(temp.split(" ")));
		ArrayList<String> ret = new ArrayList<String>();
		ret.add(args.get(3).substring(0,args.get(3).trim().indexOf(".")));
		ret.add(args.get(3).substring(args.get(3).trim().indexOf(".")+1,args.get(3).trim().length()));
		return ret;
	}

	public static ArrayList<String> getDelimitByPeriod(String column){
		ArrayList<String> ret = new ArrayList<String>();
		ret.add(column.substring(0,column.trim().indexOf(".")));
		ret.add(column.substring(column.trim().indexOf(".")+1,column.trim().length()));
		return ret;
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

	public static String getNaturalJoinColumn(String table1, String table2){
		if( ( table1.equals("Users") && table2.equals("Rating") ) || ( table1.equals("Rating") && table2.equals("Users") )  ) return "userid";
		if( ( table1.equals("Movies") && table2.equals("Rating") ) || ( table1.equals("Rating") && table2.equals("Movies") )  ) return "movieid";
		else return "zipcode";
	}

	public static ArrayList<String> getNaturalJoinPair(String column){
		ArrayList<String> ret = new ArrayList<String>();
		if(Structure.getColumnNumber("Users", column) != -1) { ret.add("Users"); ret.add(column); return ret; }
		if(Structure.getColumnNumber("Movies", column) != -1) { ret.add("Movies"); ret.add(column); return ret; }
		if(Structure.getColumnNumber("Rating", column) != -1) { ret.add("Rating"); ret.add(column); return ret; }
		else { ret.add("Rating"); ret.add(column); return ret; }
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
		return ret;
	}

	public static boolean containsAggregator(String query){
		return query.contains("COUNT") || query.contains("AVG") || query.contains("MIN") || query.contains("MAX") || query.contains("SUM");
	}

	public static int findCase(String query){
		if(query.contains("LEFT OUTER JOIN")) return 6;
		if(query.contains("INNER JOIN")) return 5;
		if(query.contains("NATURAL JOIN")) return 4;
		if(containsAggregator(query)){
			if(getHaving(query).get(0).contains("(")) return 2;
			else return 3;
		}
		return 1;
}

	public static void main(String[] args){
 		String query = "SELECT occupation, gender FROM Users WHERE age > 20 INNER JOIN Zipcodes ON Users.zipcode = Zipcodes.zipcode";
		ArrayList<String> a = getJoinColumnTable1(query);
		ArrayList<String> b = getJoinColumnTable2(query);
		System.out.println(a.size());
		//System.out.println("+"+a.get(1).substring(0,a.get(1).indexOf("."))+"+");
		for(int i=0; i<2; i++){
			System.out.println("+"+a.get(i)+"+");
		}
		for(int i=0; i<2; i++){
			System.out.println("+"+b.get(i)+"+");
		}
	}
}
