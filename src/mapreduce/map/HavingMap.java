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

public class HavingMap extends Mapper<Text, Text, Text, Text> {
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String query = conf.get("query");

		ArrayList<String> clause = Parser.getHaving(query);

		if(Parser.findCase(query) == 2) {
			Double aggregatorValue = Double.parseDouble(value.toString());
			
			boolean doesItSatisfy = false;
			switch(clause.get(1)){
				case "=":
					doesItSatisfy = aggregatorValue == Double.parseDouble(clause.get(2));
					break;
				case "<":
					doesItSatisfy = aggregatorValue <  Double.parseDouble(clause.get(2));
					break;
				case ">":
					doesItSatisfy = aggregatorValue >  Double.parseDouble(clause.get(2));
					break;
				case "<=":
					doesItSatisfy = aggregatorValue <= Double.parseDouble(clause.get(2));
					break;
				case ">=":
					doesItSatisfy = aggregatorValue >=  Double.parseDouble(clause.get(2));
					break;
				case "<>":
					doesItSatisfy = aggregatorValue != Double.parseDouble(clause.get(2));
					break;
			}

			//if(doesItSatisfy) context.write(key, value);
			if(doesItSatisfy) context.write(key, new Text(key.toString()+","+value.toString()));
		}
		else{
			String line = key.toString();
			ArrayList<String> fields = FormatConvertor.CSVToList(line);

			//String columnValue = fields.get(Structure.getColumnNumber(Parser.getTableName(query),clause.get(0)));	
			String columnValue = fields.get(Parser.getGroupBy(query).indexOf(clause.get(0)));	
			

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
					doesItSatisfy = Parser.testInCondition(columnValue, clause.get(2));
					break;
			}

			//if(doesItSatisfy) context.write(key, value);
			if(doesItSatisfy) context.write(key, new Text(key.toString()+","+value.toString()));
		}
	}
}
