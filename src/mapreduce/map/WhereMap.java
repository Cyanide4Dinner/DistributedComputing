import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WhereMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	String query;

	@Override 
	public void configure(JobConf job){
		super.configure(job);
		query = job.get("query");
	} 
	
	@Override
	public void  map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
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
					break;
				case "IN":
					break;
			}

			if(doesItSatisfy) output.collect(new Text(line), new Text(fields.get(2)));
	}
}
