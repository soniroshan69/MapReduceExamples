package com.test.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JoinMultiFile extends Configured implements Tool
{
	public static class Map1 extends Mapper<LongWritable,Text,Text,TextPair> {

		Text keyEmit = new Text();

		@Override
		public void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException
		{
			System.out.println("inside map1 "+value);
			String line=value.toString();
			String[] words=line.split("\t");
			keyEmit.set(words[0]);
			TextPair valEmit = new TextPair(1+Integer.valueOf(words[0]),words[1]);
			context.write(keyEmit, valEmit);
		}
	}

	public static class Map2 extends Mapper<LongWritable,Text,Text,TextPair> {

		Text keyEmit = new Text();
		@Override
		public void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException
		{
			System.out.println("inside map2 "+value);
			String line=value.toString();
			String[] words=line.split(" ");
			keyEmit.set(words[0]);
			TextPair valEmit = new TextPair(100+Integer.valueOf(words[0]),words[1]);
			context.write(keyEmit, valEmit);
		}
	}

	public static class Reduce extends Reducer<Text,TextPair,Text,Text> {
		Text valEmit = new Text();
		public void reduce(Text key, Iterable<TextPair> values, Context context) throws IOException, InterruptedException
		{
			System.out.println("inside reduce key "+key);
			
			
			  List<TextPair> sortedValues = new ArrayList<TextPair>();
			  for (TextPair v : values) {
				  System.out.println("inside for "+v.toString());
				  sortedValues.add(v);
			  }
			  
			 Collections.sort(sortedValues);
			 
			
			int i =1;
			String merge = "";
			for(TextPair value : sortedValues)
			{
				System.out.println("inside reduce value "+value);
				merge += value.getSecond().toString();
				if(sortedValues.size() != i) {
					merge +=", ";
				}
				i++;
			}
			valEmit.set(merge);
			context.write(key, valEmit);
		}
	}

	public static class TextPair implements WritableComparable<TextPair> {

		private IntWritable first;
		private Text second;

		public TextPair(IntWritable first, Text second) {
			set(first, second);
		}

		public TextPair() {
			set(new IntWritable(), new Text());
		}

		public TextPair(Integer first, String second) {
			set(new IntWritable(first), new Text(second));
		}

		public IntWritable getFirst() {
			return first;
		}

		public Text getSecond() {
			return second;
		}

		public void set(IntWritable first, Text second) {
			this.first = first;
			this.second = second;
		}

		public void readFields(DataInput in) throws IOException {
			first.readFields(in);
			second.readFields(in);
		}

		public void write(DataOutput out) throws IOException {
			first.write(out);
			second.write(out);
		}

		@Override
		public String toString() {
			return first + " " + second;
		}

		public int compareTo(TextPair tp) {
			int cmp = first.compareTo(tp.first);

			if (cmp != 0) {
				return cmp;
			}

			return second.compareTo(tp.second);
		}

		@Override
		public int hashCode(){
			return first.hashCode()*163 + second.hashCode();
		}

		@Override
		public boolean equals(Object o)
		{
			if(o instanceof TextPair)
			{
				TextPair tp = (TextPair) o;
				return first.equals(tp.first) && second.equals(tp.second);
			}
			return false;
		}

	} 

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new JoinMultiFile(), args);
		System.exit(exitCode);

	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = this.getConf();
		conf.setBoolean("mapreduce.map.output.compress", true);
		conf.set("mapred.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
		Job job = new Job(conf, "word count");
		job.setJarByClass(JoinMultiFile.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextPair.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Map1.class);
		MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class, Map2.class);

		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		int returnValue = job.waitForCompletion(true) ? 0:1;

		if(job.isSuccessful()) {
			System.out.println("Job was successful");
		} else if(!job.isSuccessful()) {
			System.out.println("Job was not successful");           
		}

		return returnValue;
	}
}