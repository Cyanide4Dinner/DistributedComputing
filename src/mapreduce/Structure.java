import java.util.*; 


public class Structure {
	
	public static String query = "SELECT age FROM Users WHERE age > 20";

	public static void main(String[] args){
		//String str = "age,gender,occupation";
		//ArrayList<String> slist = FormatConvertor.CSVToList(str);
		//for(int i=0;i<slist.size();i++) System.out.println(slist.get(i));
		//String str2 = FormatConvertor.ListToCSV(slist);
		ArrayList<String> selectColumns = Parser.getSelectColumns(query);
		for(int i=0;i<selectColumns.size(); i++){
			System.out.println("+"+selectColumns.get(i)+"+");
			System.out.println("+"+selectColumns.get(i).trim()+"+");
			//System.out.println(Structure.getColumnNumber(Parser.getTableName(query), selectColumns.get(i)));
		}
	}

	public static int getColumnNumber(String tableName, String columnName){
		int ret = 0;
		ArrayList<String> columns;
		switch(tableName){
			case "Users":
				columns = new ArrayList<String>(Arrays.asList(
							"userid",
							"age",
							"gender",
							"occupation",
							"zipcode"
							));
				ret = columns.indexOf(columnName);
				break;
			case "Zipcodes":
				columns = new ArrayList<String>(Arrays.asList(
							"zipcode",
							"zipcodetype",
							"city",
							"state"
							));
				ret = columns.indexOf(columnName);
				break;
			case "Movies":
				columns = new ArrayList<String>(Arrays.asList(
							"movieid", 
							"title", 
							"releasedate", 
							"unknown",
							"Action",
							"Adventure",
							"Animation",
							"Children",
							"Comedy",
							"Crime",
							"Documentary",
							"Drama",
							"Fantasy",
							"Film_Noir",
							"Horror",
							"Musical",
							"Mystery",
							"Romance",
							"Sci_Fi",
							"Thriller",
							"War",
							"Western"
							));
				ret = columns.indexOf(columnName);
				break;
			case "Rating":
				columns = new ArrayList<String>(Arrays.asList(
							"userid", 
							"movieid", 
							"rating", 
							"timestamp" 
							));
				ret = columns.indexOf(columnName);
				break;
		}
		return ret;
	}
}
