package com.mapred.driver;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordSizeCount {

	public static class WordSizeMapper extends Mapper<LongWritable,Text,IntWritable,IntWritable>{
		
		public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer tokens = new StringTokenizer(value.toString());
			while(tokens.hasMoreTokens()){
				context.write(new IntWritable(tokens.nextToken().length()),new IntWritable(1));
				
			}
		}
	}
	
	public static class WordSizeReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
		public void reduce(IntWritable key,Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable val : values){
				sum+=val.get();
			}
			context.write(key,new IntWritable(sum));
		}
		
	}
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"WordSizecount");
		
		job.setJarByClass(WordSizeCount.class);
		job.setMapperClass(WordSizeMapper.class);
		job.setReducerClass(WordSizeReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}
	
}
