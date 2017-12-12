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


public class crime {
    
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
       
       private Text location = new Text();
       private final static IntWritable one = new IntWritable(1);    
       public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
           String line = value.toString();
           String str[]=line.split(",");
           String phase = "";
           
           String area = str[5].trim();
           int time = Integer.parseInt(str[3]);
           
           if (time > 2100 && time < 500) {
        	   phase = "Night";
           } else if (time > 500 && time < 900) {
        	   phase = "Early Morning";
           } else if (time > 900 && time < 1200) {
        	   phase = "Late Morning";
           } else if (time > 1200 && time < 1600) {
        	   phase = "Early Afternoon";
           } else if (time > 1600 && time < 1800) {
        	   phase = "Late Afternoon";
           } else {
        	   phase = "Evening";
           }
        	    
           String crime = area +","+ phase;
                    location.set(crime);
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
        Job job = Job.getInstance(conf, "time");
        job.setJarByClass(crime.class);
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
