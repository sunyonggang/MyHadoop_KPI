package com.test.mr;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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

public class KPIPV {

  public static class MyPVMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private  IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException 
    {
    	
    	KPI kpi = KPI.filterPVs(value.toString());
    	if(kpi.isValid())
    	{
    		word.set(kpi.getRequest());
    		context.write(word, one);
    	}
    }
  }
  
  public static class MyPVReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for(IntWritable val : values)
      {
    	  sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
	


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: KPIPV <in> <out>");
      System.exit(2);
    }
//    String input = "hdfs://192.168.199.101:9000/user/grid/log_kpi/";
//    String output = "hdfs://192.168.199.101:9000/user/grid/pv_count";
    
    Job job = new Job(conf, "PV count");
    job.setJarByClass(KPIPV.class);
    job.setMapperClass(MyPVMapper.class);
//    job.setCombinerClass(PVReducer.class);
    job.setReducerClass(MyPVReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
