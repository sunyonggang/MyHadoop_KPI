package com.test.mr;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

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

public class KPIIP {

  public static class MyIPMapper 
       extends Mapper<Object, Text, Text, Text>{		//this value is Text
    
    private Text hello = new Text();
    private Text world = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
     KPI kpi = KPI.filterIPs(value.toString());
      if (kpi.isValid()) {
        hello.set(kpi.getRequest());
        world.set(kpi.getRemote_addr());
        context.write(hello, world);
      }
    }
  }
  
  public static class MyIPReducer 
       extends Reducer<Text,Text,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      Set<String> set = new HashSet<String>();
      
      for (Text val : values) {
          set.add(val.toString());
        }
      result.set(set.size());
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: KPIIP <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "IP count");
    job.setJarByClass(KPIIP.class);
    job.setMapperClass(MyIPMapper.class);
//    job.setCombinerClass(MyIPReducer.class);
    job.setReducerClass(MyIPReducer.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);		//beacuse you choose value is Text, there also should fix
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
