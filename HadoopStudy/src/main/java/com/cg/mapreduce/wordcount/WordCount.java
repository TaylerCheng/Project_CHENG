package com.cg.mapreduce.wordcount;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import com.cg.utils.EJob;

public class WordCount {

	public Logger logger = Logger.getLogger(WordCount.class);

	private static Job initJob(Configuration conf) {
		conf.addResource(new Path(
				"D:/program/hadoop-2.6.0/etc/hadoop/core-site.xml"));
		conf.addResource(new Path(
				"D:/program/hadoop-2.6.0/etc/hadoop/hdfs-default.xml"));
		conf.addResource(new Path(
				"D:/program/hadoop-2.6.0/etc/hadoop/hdfs-site.xml"));
		conf.addResource(new Path(
				"D:/program/hadoop-2.6.0/etc/hadoop/yarn-default.xml"));
		conf.addResource(new Path(
				"D:/program/hadoop-2.6.0/etc/hadoop/yarn-site.xml"));
		conf.addResource(new Path(
				"D:/program/hadoop-2.6.0/etc/hadoop/mapred-site.xml"));
		conf.set("HADOOP_USER_NAME", "hadoop");
		conf.set("mapred.reduce.tasks", "2");
		Job job = null;
		try {
			File jarFile = EJob.createTempJar("bin");
			EJob.addClasspath("D:/program/hadoop-2.6.0/etc/hadoop/");
			ClassLoader classLoader = EJob.getClassLoader();
			Thread.currentThread().setContextClassLoader(classLoader);
			job = new Job(conf, "wordcount");
			((JobConf) job.getConfiguration()).setJar(jarFile.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return job;
	}

	public static void main(String[] args) throws Exception {
		// section 1
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("parameters should >= 2");
			System.exit(2);
		}
		Job job = null;
		String mode = "local";// local,cluster
		
		if (mode.equals("cluster")) {
			job = initJob(conf);
			if (job == null) {
				System.out.println("yarn-mode init failed!");
				System.exit(0);
			}
		} else {
			job = new Job(conf, "wordcount");
		}
		job.setJarByClass(WordCount.class);
		
		// section2
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		
		// section3
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
		Path outputPath = new Path(otherArgs[1]);
		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath);
		}
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, outputPath);
		
		// section4
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}	
}