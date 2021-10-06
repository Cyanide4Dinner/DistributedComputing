import java.util.*; 


public class Structure {
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

	public static void main(String[] args){
		System.out.println(getColumnNumber("Users","age"));
	}
}
