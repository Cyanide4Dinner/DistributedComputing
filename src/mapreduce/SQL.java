import java.io.IOException;
import java.util.*;

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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SQL extends Configured implements Tool {

	//public static String EXAMPLE_SQL = "SELECT occupation, gender, AVG(age) FROM Users WHERE age > 20 GROUP BY occupation, gender HAVING gender LIKE M";
	public static String EXAMPLE_SQL = "SELECT Users.occupation, Users.gender, Zipcodes.city FROM Users WHERE age > 20 LEFT OUTER JOIN Zipcodes ON Users.zipcode = Zipcodes.zipcode";
	//public static String EXAMPLE_SQL = "SELECT occupation, gender FROM Users WHERE age > 20 NATURAL JOIN Zipcodes";

	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new SQL(), args));
	}

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = this.getConf();

		conf.set("query", EXAMPLE_SQL);

		Job job = new Job(conf, "sql");
		job.setJarByClass(SQL.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		int sqlCase = Parser.findCase(EXAMPLE_SQL);

		if(sqlCase == 1){
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
		}

		if(sqlCase == 2 || sqlCase == 3){
			Configuration whereConf = new Configuration(false);
			ChainMapper.addMapper(job, 
						WhereMap.class,
						LongWritable.class,
						Text.class,
						Text.class,
						Text.class,
						whereConf
					);
			Configuration groupByMapConf = new Configuration(false);
			ChainMapper.addMapper(job,
						GroupByMap.class,
						Text.class,
						Text.class,
						Text.class,
						Text.class,
						groupByMapConf
					);
			Configuration groupByReduceConf = new Configuration(false);
			ChainReducer.setReducer(job,
					GroupByReduce.class,
					Text.class,
					Text.class,
					Text.class,
					Text.class,
					groupByReduceConf
					);

			Configuration havingReduceConf = new Configuration(false);
			ChainReducer.addMapper(job,
					HavingMap.class,
					Text.class,
					Text.class,
					Text.class,
					Text.class,
					havingReduceConf
					);
		}

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
		if(sqlCase <= 3){
			FileInputFormat.setInputPaths(job, tablePath);
		}
		
		if(sqlCase > 3){
			Configuration whereConf = new Configuration(false);
			MultipleInputs.addInputPath(job, new Path(tablePath), TextInputFormat.class, WhereMapJoin.class);
			String joinTablePath = "/tmp/users.csv";
			switch(Parser.getJoinTableName(EXAMPLE_SQL)){
				case "Movies":
					joinTablePath = "input/movies.csv";
					break;
				case "Users":
					joinTablePath = "input/users.csv";
					break;
				case "Zipcodes":
					joinTablePath = "input/zipcodes.csv";
					break;
				case "Rating":
					joinTablePath = "input/rating.csv";
					break;
			}
			MultipleInputs.addInputPath(job, new Path(joinTablePath), TextInputFormat.class, JoinTableMap.class);
			job.setReducerClass(JoinReduce.class);
		}

		FileOutputFormat.setOutputPath(job, new Path(args[0]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
