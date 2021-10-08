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

public class JoinTableMap extends Mapper<LongWritable, Text, Text, Text> {

	public void  map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			String query = conf.get("query");

			String line = value.toString();
			ArrayList<String> fields = FormatConvertor.CSVToList(line);

			int sqlCase = Parser.findCase(query);

			String joinColumn;
			if(sqlCase == 4) joinColumn = Parser.getNaturalJoinColumn( Parser.getTableName(query), Parser.getJoinTableName(query) );
			else joinColumn = Parser.getJoinColumnTable2(query).get(1);
			context.write(new Text(fields.get(Structure.getColumnNumber(Parser.getJoinTableName(query), joinColumn))), new Text("B"+line));
	}
}
