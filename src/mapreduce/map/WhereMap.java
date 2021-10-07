import java.io.IOException;
import java.util.*;
import java.util.regex.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class WhereMap extends Mapper<LongWritable, Text, Text, Text> {

	/*
	@Override 
	public void configure(JobConf job){
		super.configure(job);
		query = job.get("query");
	} 
	*/
	
	//String query = "SELECT age FROM Users WHERE age > 20";

	public void  map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			String query = conf.get("query");
			//System.out.println(query2);

			//0 - columnName, 1 - operator, 2 - value
			ArrayList<String> clause = Parser.getWhere(query); 
			String line = value.toString();
			ArrayList<String> fields = FormatConvertor.CSVToList(line);

			String columnValue = fields.get(Structure.getColumnNumber(Parser.getTableName(query),clause.get(0)));	

			boolean doesItSatisfy = false;
			switch(clause.get(1)){
				case "=":
					doesItSatisfy = Integer.parseInt(columnValue) == Integer.parseInt(clause.get(2));
					break;
				case ">":
					doesItSatisfy = Integer.parseInt(columnValue) > Integer.parseInt(clause.get(2));
					break;
				case "<":
					doesItSatisfy = Integer.parseInt(columnValue) < Integer.parseInt(clause.get(2));
					break;
				case "<=":
					doesItSatisfy = Integer.parseInt(columnValue) <= Integer.parseInt(clause.get(2));
					break;
				case ">=":
					doesItSatisfy = Integer.parseInt(columnValue) >= Integer.parseInt(clause.get(2));
					break;
				case "<>":
					doesItSatisfy = Integer.parseInt(columnValue) != Integer.parseInt(clause.get(2));
					break;
				case "LIKE":
					doesItSatisfy = Pattern.compile(clause.get(2)).matcher(columnValue).matches();
					break;
				case "IN":
					break;
			}

			if(doesItSatisfy) context.write(new Text(line), new Text(line));
	}
}
