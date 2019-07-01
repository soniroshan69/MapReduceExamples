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

public class PartitionerExample extends Configured implements Tool{

	public static class Map extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key, Text value, Context context)
		{
			System.out.println("key "+key);
			System.out.println("this "+this.toString());
			System.out.println("this class"+this.getClass().toString());
			
			try{
				if(key.get()==0) {
					return;
				}
				String[] str = value.toString().split("\t", -3);
				String gender=str[3];
				context.write(new Text(gender), new Text(value));
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
	}

	public static class Reduce extends Reducer<Text,Text,Text,IntWritable>
	{
		public int max = -1;
		public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
		{

			System.out.println("key is "+key);
			System.out.println("this "+this.toString());
			System.out.println("this class "+this.getClass());

			max = -1;

			for (Text val : values)
			{
				String [] str = val.toString().split("\t", -3);
				if(Integer.parseInt(str[4])>max)
					max=Integer.parseInt(str[4]);
			}

			context.write(new Text(key), new IntWritable(max));
		}
	}

	public static class AgePartitioner extends
	Partitioner < Text, Text >
	{
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks)
		{
			System.out.println("partitioner "+this.toString());
			System.out.println("partitioner called key"+key);
			String[] str = value.toString().split("\t");
			int age = Integer.parseInt(str[2]);

			if(numReduceTasks == 0)
			{
				return 0;
			}

			if(age<=20)
			{
				return 0;
			}
			else if(age>20 && age<=30)
			{
				return 1 % numReduceTasks;
			}
			else
			{
				return 2 % numReduceTasks;
			}
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new PartitionerExample(), args);
        System.exit(exitCode);
		
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = this.getConf();
		Job job = new Job(conf, "partitioner program");
		job.setJarByClass(PartitionerExample.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setPartitionerClass(AgePartitioner.class);
		job.setCombinerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(3);
		//Configuring the input/output path from the filesystem into the job
		//FileInputFormat.setMinInputSplitSize(job, 2);
		FileInputFormat.setMaxInputSplitSize(job, 100);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//deleting the output path automatically from hdfs so that we don't have to delete it explicitly

		//exiting the job only if the flag value becomes false
		int returnValue = job.waitForCompletion(true) ? 0:1;
        
        if(job.isSuccessful()) {
            System.out.println("Job was successful");
        } else if(!job.isSuccessful()) {
            System.out.println("Job was not successful");           
        }
         
        return returnValue;
	}
}
