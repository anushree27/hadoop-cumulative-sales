package sales;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class analyze {
	public static void main(String [] args) throws Exception
	{
	Configuration c=new Configuration();
	String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
	Path input=new Path(files[0]);
	Path output=new Path(files[1]);
	Job j=new Job(c,"wordcount");
	j.setJarByClass(analyze.class);
	j.setMapperClass(MapForWordCount.class);
	j.setReducerClass(ReduceForWordCount.class);
	j.setOutputKeyClass(Text.class);
	j.setOutputValueClass(Text.class);
	FileInputFormat.addInputPath(j, input);
	FileOutputFormat.setOutputPath(j, output);
	System.exit(j.waitForCompletion(true)?0:1);
	}
	public static class MapForWordCount extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
	{
		String line = value.toString();
		int a = line.length();
		if(a > 2)
		{
			String[] words = line.split("\\s+");
			String sales = words[1];
			String prod = words[2];
			con.write(new Text(prod), new Text(sales));
		}
	}
	}
	public static class ReduceForWordCount extends Reducer<Text, Text, Text, FloatWritable>
	{
	public void reduce(Text word, Iterable<Text> values, Context con) throws IOException, InterruptedException
	{
		HashMap<String, Long> userIdSet = new HashMap<String, Long>();
		Text prod;
		Long tempSales;
		for (Text value: values) {
			tempSales = Long.parseLong(value.toString());
			prod = word;

			if( userIdSet.containsKey(prod.toString()))
			{
				Long count = (Long)userIdSet.get(prod.toString());
		        userIdSet.put(prod.toString(), new Long(count.intValue() + tempSales));
			}
			else
			{
				userIdSet.put(prod.toString(), tempSales);
			}

		}
		for (String str : userIdSet.keySet() ) {
			String key = str.toString();
			Long value = userIdSet.get(key);
			Text keyText1 = new Text ("Product :" + key + "\n" + "Total Sales: ");
			con.write(keyText1, new FloatWritable(value));
		}

	}
	}

}
