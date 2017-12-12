import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class crime2 {
    
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
       
       private Text location = new Text();
       private final static IntWritable one = new IntWritable(1);    
       public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
           String line = value.toString();
           String str[]=line.split(",");
           
           String premises = str[14].trim();
           location.set(premises);
           context.write(location, one);     
      }        	
    } 
           
    public static class Reduce extends Reducer<Text,IntWritable, Text, String> {
   
       public void reduce(Text key, Iterable<IntWritable> values, Context context) 
         throws IOException, InterruptedException {
    	   
           int sum = 0;
           for (IntWritable val : values) {
               
               sum += val.get();
           }
           String total = "," + Integer.toString(sum);
           context.write(key, total);
       }
    }
           
    @SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
        
        @SuppressWarnings("deprecation")
        Job job = Job.getInstance(conf, "premises");
        job.setJarByClass(crime2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setCombinerClass(Reducer.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
           
  }

