package com.test.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PartitionerAndCombinerExample extends Configured implements Tool{
	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable> {
		int mapperCount = 1;
		IntWritable token = new IntWritable(1);
		@Override
		public void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException{
			System.out.println("mapperCount "+mapperCount++);
			System.out.println("key "+key +" class "+this.toString());
			String[] words = value.toString().split(" ");
			for(String word : words) {
				value.set(word);
				context.write(value, token);
			}
		}
	}

	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
		int recucerCount = 1;
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException,InterruptedException {
			System.out.println("recucerCount "+recucerCount++);
			System.out.println("inside reducer key "+ this.toString());
			System.out.println("inside reducer key "+ key);			
			int sum=0;
			for(IntWritable x: values)
			{
				System.out.println("inside reducer value "+ x.get());
				sum+=x.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static class Combine extends Reducer<Text,IntWritable,Text,IntWritable> {
		int combinerCount = 1;
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException,InterruptedException {
			System.out.println("combinerCount "+combinerCount++);
			System.out.println("inside combiner key "+ this.toString());
			System.out.println("inside combiner key "+ key);
			int sum=0;
			for(IntWritable x: values)
			{
				System.out.println("inside combiner value "+ x.get());
				sum+=x.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static class Partition extends Partitioner<Text, IntWritable> {
		int partitionerCount = 1;
		int count = 0;
		@Override
		public int getPartition(Text key, IntWritable value, int numReducers) {
			System.out.println("partitionerCount "+partitionerCount++);
			System.out.println("inside partitioner key "+ this.toString());
			System.out.println("inside partitioner key "+ key);
			System.out.println("inside partitioner value "+ value);
			System.out.println("inside partitioner count "+ count);
			if(numReducers==0) {
				return 0;
			}else {
				if(count==0) {
					count += 1;
					return 0;
				}else {
					count -= 1;
					return 1;
				}
			}
		}		

	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new PartitionerAndCombinerExample(), args);
        System.exit(exitCode);
		
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = this.getConf();
		Job job = new Job(conf, "partitioner with combiner example");
		job.setJarByClass(PartitionerAndCombinerExample.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setPartitionerClass(Partition.class);
		job.setCombinerClass(Combine.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(3);
		FileInputFormat.setMaxInputSplitSize(job, 50);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		int returnValue = job.waitForCompletion(true) ? 0:1;

		if(job.isSuccessful()) {
			System.out.println("Job was successful");
		} else if(!job.isSuccessful()) {
			System.out.println("Job was not successful");           
		}

		return returnValue;
	}
}
