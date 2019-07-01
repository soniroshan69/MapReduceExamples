package com.test.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

public class DistributedCacheExample extends Configured implements Tool{
	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable> {

		IntWritable token = new IntWritable(1);
		private Set<String> stopWords = new HashSet<String>();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			try{
				Path[] stopWordsFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				if(stopWordsFiles != null && stopWordsFiles.length > 0) {
					for(Path stopWordFile : stopWordsFiles) {
						readFile(stopWordFile);
					}
				}
			} catch(IOException ex) {
				System.err.println("Exception in mapper setup: " + ex.getMessage());
			}
		}

		@Override
		public void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException{

			System.out.println("keyyyyy "+key +" class "+this.toString());
			String[] words = value.toString().split(" ");
			for(String word : words) {
System.out.println("sw size "+stopWords.size());
				if(!stopWords.contains(word)) {
					value.set(word);
					context.write(value, token);
				}
			}
		}

		private void readFile(Path filePath) {
			try{
				BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
				String stopWord = null;
				while((stopWord = bufferedReader.readLine()) != null) {
					System.out.println("adding to sw "+stopWord.toLowerCase());
					stopWords.add(stopWord.toLowerCase());
				}
				bufferedReader.close();
			} catch(IOException ex) {
				System.err.println("Exception while reading stop words file: " + ex.getMessage());
			}
		}
	}

	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException,InterruptedException {
			System.out.println("inside reducer key "+ this.toString());
			System.out.println("inside reducer key "+ key);
			int sum=0;
			for(IntWritable x: values)
			{
				sum+=x.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new DistributedCacheExample(), args);
		System.exit(exitCode);

	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = this.getConf();
		Job job = new Job(conf, "word count distributed cache");
		job.setJarByClass(this.getClass());
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(2);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		DistributedCache.addCacheFile(new Path(args[2]).toUri(), job.getConfiguration());
		
		int returnValue = job.waitForCompletion(true) ? 0:1;

		if(job.isSuccessful()) {
			System.out.println("Job was successful");
		} else if(!job.isSuccessful()) {
			System.out.println("Job was not successful");           
		}

		return returnValue;
	}
}
