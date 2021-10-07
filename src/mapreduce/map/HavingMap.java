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

		if(doesItSatisfy) context.write(key, value);
	}
}
