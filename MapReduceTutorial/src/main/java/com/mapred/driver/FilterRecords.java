package com.mapred.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.mapred.driver.GroupByCarrier.AirLineMapper;

public class FilterRecords {
	
	public static enum RECORDS{
		ATL,
		MCI,
		LAX
	}
	
public static class AirPortFilter extends Mapper<LongWritable,Text,Text,Text>{
		
		Text word = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] cols = value.toString().split(",");
			if(cols[16].equals("ATL")){
				context.getCounter(RECORDS.ATL).increment(1);
			}
			if(cols[16].equals("MCI")){
				context.getCounter(RECORDS.ATL).increment(1);
			}
			if(cols[16].equals("LAX")){
				context.getCounter(RECORDS.ATL).increment(1);
			}
			if(cols[0].equals("2006"))
			context.write(null, value);
		
		}
		
	}
	
	public static void main(String args[]) throws Exception{
		
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    job.setNumReduceTasks(0);//--To be shown
	    job.setJarByClass(WordCount.class);
	    job.setMapperClass(AirPortFilter.class);
	    //job.setCombinerClass(AirLineReducer.class);
	    //job.setReducerClass(AirLineReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	    
	    
	}

}
