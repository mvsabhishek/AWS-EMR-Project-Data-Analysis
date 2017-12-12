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


public class crime1 {
    
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
       
       private Text location = new Text();
       private final static IntWritable one = new IntWritable(1);    
       public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
           
       String line = value.toString();
       String str[]=line.split(",");
       String agegroup = "";
       
       String sex = str[11].trim();
       int age = Integer.parseInt("0" + str[10]);
    	  
    	   
       if (age >= 0 && age <= 10 ){
      			agegroup = "0-10";
     } else if (age > 10 && age <= 20 ){
      			agegroup = "10-20";
     } else if (age > 20 && age <= 30 ){
      			agegroup = "20-30";
     } else if (age > 30 && age <= 40 ){
      			agegroup = "30-40";
     } else if (age > 40 && age <= 50 ){
      			agegroup = "40-50";
     } else if (age > 50 && age <= 60 ){
      			agegroup = "50-60";
     } else if (age > 60 && age <= 70 ){
      			agegroup = "60-70";
     } else if (age > 70 && age <= 80 ){
      			agegroup = "70-80";
     } else if (age > 80 && age <= 90 ){
			agegroup = "80-90";
     }
    	   
       if (sex.equals("F"))	  {
           	sex = "Female"; 
           	String people = sex +","+ agegroup;
               location.set(people);
               context.write(location, one);  
     } else if (sex.equals("M"))	{
           	sex = "Male";
           	String people = sex +","+ agegroup;
            location.set(people);
            context.write(location, one);  
     }
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
        Job job = Job.getInstance(conf, "females_survived");
        job.setJarByClass(crime1.class);
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
