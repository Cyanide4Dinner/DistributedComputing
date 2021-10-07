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

public class GroupByMap extends Mapper<Text,Text,Text,Text> {

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String query = conf.get("query");

		ArrayList<String> groupByColumns = Parser.getGroupBy(query);
		String line = value.toString();
		ArrayList<String> fields = FormatConvertor.CSVToList(line);

		ArrayList<String> res = new ArrayList<String>();
		for(int i=0; i<groupByColumns.size(); i++){
			res.add(fields.get(Structure.getColumnNumber(Parser.getTableName(query), groupByColumns.get(i))));
		}
		context.write(new Text(FormatConvertor.ListToCSV(res)), new Text(line));
	}
}
