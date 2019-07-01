package com.test.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FirstAndLastLine extends Configured implements Tool{

	public static class Map extends Mapper<LongWritable,Text, LongWritable, Text> {

		boolean firstLine = true;
		long lastKey = -1;
		String lastLine = "";

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException{

			System.out.println("key "+key +" class "+this.toString());
			System.out.println("inside map value "+ value.toString());

			if(firstLine) {
				context.write(key, value);
				firstLine = false;
			}else if(key.get()>lastKey){
				lastKey = key.get();
				lastLine = value.toString();
				System.out.println("inside map lasLine "+ lastLine);
			}
		}
		
		@Override
		protected void cleanup(Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
			
			if(lastKey!=-1) {
			System.out.println("lastkey "+ lastKey);
			System.out.println("lastLine "+ lastLine);
			context.write(new LongWritable(lastKey), new Text(lastLine));
			}
		}
	}

	public static class Reduce extends Reducer<LongWritable, Text, Text, NullWritable> {
		
		boolean firstLine = true;
		long lastKey = -1;
		Text lastLine = null;
		public void reduce(LongWritable key, Iterable<Text> values,Context context) throws IOException,InterruptedException {
			System.out.println("inside reducer key "+ key);
			String value = values.iterator().next().toString();
			System.out.println("inside reducer value "+value);
			
			if(firstLine) {
				context.write(new Text(value), NullWritable.get());		
				firstLine = false;
			}else if(key.get()>lastKey){
				lastKey = key.get();
				lastLine = new Text(value);
			}
		}
		
		@Override
		protected void cleanup(Reducer<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
			System.out.println("reducer lastkey "+ lastKey);
			context.write(lastLine, NullWritable.get());
		}
	}

	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new FirstAndLastLine(), args);
        System.exit(exitCode);
		
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = this.getConf();
		Job job = new Job(conf, "My first and last line Program");
		job.setJarByClass(FirstAndLastLine.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);
		//Configuring the input/output path from the filesystem into the job
		//FileInputFormat.setMinInputSplitSize(job, 2);
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
