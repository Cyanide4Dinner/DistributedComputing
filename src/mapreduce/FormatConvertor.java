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
		ArrayList<String> res = new ArrayList(Arrays.asList(str.split(",")));
		ArrayList<String> ret = new ArrayList<String>();
		for(int i=0;i<res.size();i++){
			if(res.get(i).charAt(0) == '"'){
				ret.add(res.get(i).substring(1,res.get(i).length()-1));
			}
			else{
				ret.add(res.get(i));
			}
		}
		return ret;
	}
}
