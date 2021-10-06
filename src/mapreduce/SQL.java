import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class SQL {

	public static String EXAMPLE_SQL = "SELECT age FROM Users WHERE age > 20";

	/*
	public static class whereMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		public void  map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			ArrayList<String> fields = FormatConvertor.CSVToList(line);
			output.collect(new Text(line), new Text(fields.get(2)));
		}
	}
	*/

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(SQL.class);
		conf.setJobName("sql");

		conf.set("query", EXAMPLE_SQL);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(WhereMap.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		String tablePath = "/tmp/users.csv";
		switch(Parser.getTableName(EXAMPLE_SQL)){
			case "Movies":
				tablePath = "input/movies.csv";
				break;
			case "Users":
				tablePath = "input/users.csv";
				break;
			case "Zipcodes":
				tablePath = "input/zipcodes.csv";
				break;
			case "Rating":
				tablePath = "input/rating.csv";
				break;
		}

		FileInputFormat.setInputPaths(conf, tablePath);
		FileOutputFormat.setOutputPath(conf, new Path(args[0]));

		JobClient.runJob(conf);
	}
}
