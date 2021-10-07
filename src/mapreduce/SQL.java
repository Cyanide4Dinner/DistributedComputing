import java.io.IOException;
import java.util.*;

/*
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.Job;
*/

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SQL extends Configured implements Tool {

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
	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new SQL(), args));
	}

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = this.getConf();

		conf.set("query", "SELECT age FROM Users WHERE age > 20");

		Job job = new Job(conf, "sql");
		job.setJarByClass(SQL.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);


		//JobConf conf = new JobConf(SQL.class);
		//conf.set("query", EXAMPLE_SQL);
		//conf.setInputFormat(TextInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);

		//Job job = Job.getInstance(conf, "sql");
		//job.setJarByClass(SQL.class);


		//JobConf conf = new JobConf(SQL.class);
		//conf.setJobName("sql");


		//conf.setOutputKeyClass(Text.class);
		//conf.setOutputValueClass(Text.class);

		//conf.setMapperClass(WhereMap.class);
		
		//Job job = new Job(conf);

		Configuration whereConf = new Configuration(false);
		ChainMapper.addMapper(job, 
					WhereMap.class,
					LongWritable.class,
					Text.class,
					Text.class,
					Text.class,
					whereConf
				);

		Configuration selectConf = new Configuration(false);
		ChainMapper.addMapper(job,
					SelectMap.class,
					Text.class,
					Text.class,
					Text.class,
					Text.class,
					selectConf
				);

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

		FileInputFormat.setInputPaths(job, tablePath);
		FileOutputFormat.setOutputPath(job, new Path(args[0]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
