import java.io.IOException;
import java.util.*;
import java.util.regex.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JoinReduce extends Reducer<Text,Text,Text,Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		String query = conf.get("query");
		
		Iterator<Text> itr = values.iterator();

		//Categories as from different tables
		ArrayList<String> TableA = new ArrayList<String>();				
		ArrayList<String> TableB = new ArrayList<String>();				

		while(itr.hasNext()){
			String curr = itr.next().toString();
			if(curr.charAt(0) == 'A'){
				TableA.add(curr.substring(1,curr.length()));
			}
			else{
				TableB.add(curr.substring(1,curr.length()));
			}
		}

		int sqlCase = Parser.findCase(query);
		String tableName = Parser.getTableName(query);
		String joinTableName = Parser.getJoinTableName(query);

		ArrayList<String> selectColumns = Parser.getSelectColumns(query);
		for(int i=0; i<TableA.size(); i++){
			for(int j=0; j<TableB.size(); j++){
				ArrayList<String> ret = new ArrayList<String>();
				ArrayList<String> record1 = FormatConvertor.CSVToList(TableA.get(i));
				ArrayList<String> record2 = FormatConvertor.CSVToList(TableB.get(j));
				for(int k=0;k<selectColumns.size(); k++){
					ArrayList<String> m;
					if(sqlCase == 4) m = Parser.getNaturalJoinPair(selectColumns.get(k));
					else m = Parser.getDelimitByPeriod(selectColumns.get(k));
					if(m.get(0).equals(tableName)){
						ret.add(record1.get(Structure.getColumnNumber(tableName,m.get(1))));
					}
					else{
						ret.add(record2.get(Structure.getColumnNumber(joinTableName,m.get(1))));
					}
				}
				context.write(new Text(FormatConvertor.ListToCSV(ret)), new Text(FormatConvertor.ListToCSV(ret)));
			}
			if(TableB.size() == 0 && sqlCase == 6){
				ArrayList<String> ret = new ArrayList<String>();
				ArrayList<String> record1 = FormatConvertor.CSVToList(TableA.get(i));
				for(int k=0;k<selectColumns.size(); k++){
					ArrayList<String> m;
					m = Parser.getDelimitByPeriod(selectColumns.get(k));
					if(m.get(0).equals(tableName)){
						ret.add(record1.get(Structure.getColumnNumber(tableName,m.get(1))));
					}
					else{
						ret.add("NULL");
					}
				}
			}
		}
	} 
}
