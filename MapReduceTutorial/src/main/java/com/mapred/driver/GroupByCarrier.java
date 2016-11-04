package com.mapred.driver;

import java.io.IOException;

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


public class GroupByCarrier {
	
	public static class AirLineMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		
		Text word = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			System.out.println(key);
			String[] cols = value.toString().split(",");
				context.write(new Text(cols[16]), new IntWritable(1));
		
		}
		
	}
	public static class AirLineReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,
                Context context
                ) throws IOException, InterruptedException {
					int sum = 0;
					for (IntWritable val : values) {
							sum += val.get();
					}
					result.set(sum);
					context.write(key, result);
}
	}
	
	
	public static void main(String args[]) throws Exception{
		
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    
	    job.setJarByClass(WordCount.class);
	    job.setMapperClass(AirLineMapper.class);
	    job.setCombinerClass(AirLineReducer.class);
	    job.setReducerClass(AirLineReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
