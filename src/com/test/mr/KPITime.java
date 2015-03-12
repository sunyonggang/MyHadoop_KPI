package com.test.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class KPITime
{
	public static class MyTimeMapper extends Mapper<Object, Text, Text, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		private Text time = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			KPI kpi = KPI.filterTime(value.toString());
			if(kpi.isValid())
			{
				time.set(kpi.getTime_local());
				context.write(time, one);
			}
			
		}
		
	}
	
	public static class MyTimeReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable count = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for(IntWritable val : values)
			{
				sum += val.get();
			}
			count.set(sum);
			context.write(key, count);
		}
		
	}
	
	
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2)
		{
			System.out.println("Usage: KPITime <input> <output>");
			System.exit(2);
		}
		Job job = new Job(conf, "Time count");
		job.setJarByClass(KPITime.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(MyTimeMapper.class);
		job.setCombinerClass(MyTimeReducer.class);
		job.setReducerClass(MyTimeReducer.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
