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

public class GroupByReduce extends Reducer<Text,Text,Text,Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		String query = conf.get("query");

		ArrayList<String> aggregator = Parser.getAggregator(query);
		
		int count = 0, sum = 0, max = 0, min = 0, num = 0;
		Iterator<Text> itr = values.iterator();
		switch(aggregator.get(0)){
			case "COUNT":
				while(itr.hasNext()){
					count++;
				}
				context.write(key, new Text(Integer.toString(count)));
				break;
			case "AVG":
				while(itr.hasNext()){
					sum +=  Integer.parseInt( FormatConvertor.CSVToList( itr.next().toString() ).get( Structure.getColumnNumber( Parser.getTableName(query), aggregator.get(1) ) ) );
					count++;
				}
				context.write(key, new Text(String.valueOf( (double) sum / count )));
				break;
			case "MAX":
				while(itr.hasNext()){
					num = Integer.parseInt( FormatConvertor.CSVToList( itr.next().toString() ).get( Structure.getColumnNumber( Parser.getTableName(query), aggregator.get(1) ) ) );
					if(max<num)  max = num;
				}
				context.write(key, new Text(String.valueOf(max)));
				break;
			case "MIN":
				while(itr.hasNext()){
					num = Integer.parseInt( FormatConvertor.CSVToList( itr.next().toString() ).get( Structure.getColumnNumber( Parser.getTableName(query), aggregator.get(1) ) ) );
					if(min>num)  min = num;
				}
				context.write(key, new Text(String.valueOf(min)));
				break;
			case "SUM":
				while(itr.hasNext()){
					sum +=  Integer.parseInt( FormatConvertor.CSVToList( itr.next().toString() ).get( Structure.getColumnNumber( Parser.getTableName(query), aggregator.get(1) ) ) );
				}
				context.write(key, new Text(String.valueOf(sum)));
				break;
		}
	}
} 
