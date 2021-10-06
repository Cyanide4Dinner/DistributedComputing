import java.io.IOException;
import java.util.*;


// Converts List to CSV String and vice versa

public class FormatConvertor {

	public static void main(String[] args){
		ArrayList<String> l = CSVToList("Ram,Shyam,Ramesh");
		//System.out.println(ListToCSV(l));
		for(int i=0;i<l.size();i++){
			System.out.println(l.get(i));
		}	
	}

	public static String ListToCSV(ArrayList<String> list){
		if(list.isEmpty()){
			return "";
		}
		if(list.size() == 1){
			return list.get(0);
		}
		else{
			String ret = list.get(0);	
			for(int i=1;i<list.size();i++){
				ret = ret.concat(",");
				ret = ret.concat(list.get(i));
			}
			return ret;
		}
	}

	public static ArrayList<String> CSVToList(String str){
		return new ArrayList(Arrays.asList(str.split(",")));
	}
}
