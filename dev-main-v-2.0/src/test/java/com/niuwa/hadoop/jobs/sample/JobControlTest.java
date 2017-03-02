package com.niuwa.hadoop.jobs.sample;

/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
import java.io.IOException;
import java.net.URI;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.alibaba.fastjson.JSONObject;
import com.niuwa.hadoop.chubao.rules.Rules;
import com.niuwa.hadoop.util.DateUtil;
import com.niuwa.hadoop.util.HadoopUtil;

public class JobControlTest {
	public static class UserIdMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		private Text outKey = new Text();
		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			JSONObject form = JSONObject.parseObject(value.toString());
			// condition1 客户激活时间距当前不少于3个月
			if (Rules.isMatchedRule_1(form.getLong("device_activation"))) {
				outKey.set(form.getString("user_id"));
				context.write(outKey, one);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static class AddDateMapper extends
			Mapper<Object, Text, IntWritable, Text> {
		private IntWritable  outKey= new IntWritable();
		private Text  outValue= new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String str[]=value.toString().split("\t");
			
			outKey.set(Integer.parseInt(str[1]));
			outValue.set(str[0]+"\t"+DateUtil.format(new Date()));
			
			context.write(outKey, outValue);
		}
	}
	
	public static class Job2Reducer extends
		Reducer<IntWritable,Text, Text, IntWritable> {
		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for(Text value: values){
				context.write(value,key);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		HadoopUtil.isWinOrLiux();
		Configuration conf = new Configuration();
		String path = "hdfs://ns1:9000/user/root";
		if (args.length != 0) {
			path = args[0];
		}
		String[] args_1 = new String[] {
				path + "/chubao/input/contact",
				path + "/chubao/temp/"
						+ DateUtil.format(new Date())
						+ "/contact_total",
				path + "/chubao/temp/"
						+ DateUtil.format(new Date())
						+ "/contact_total_next" };
		String[] otherArgs = new GenericOptionsParser(conf, args_1)
				.getRemainingArgs();
		// 第一个job
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(JobControlTest.class);
		job.setMapperClass(UserIdMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// 删除原有的输出
		deleteOutputFile(otherArgs[1], otherArgs[0]);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// 第二个job
		Job job2 = Job.getInstance(conf, "job2");
		job2.setJarByClass(JobControlTest.class);
		job2.setMapperClass(AddDateMapper.class);
		job2.setReducerClass(Job2Reducer.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
		// 删除原有的输出
		deleteOutputFile(otherArgs[2], otherArgs[1]);
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));

		// 创建ControlledJob
		ControlledJob controlledJob1 = new ControlledJob(job.getConfiguration());
		ControlledJob controlledJob2 = new ControlledJob(
				job2.getConfiguration());

		// 添加依赖
		controlledJob2.addDependingJob(controlledJob1);

		// 创建JobControl
		JobControl jobControl = new JobControl("JobControlDemoGroup");
		jobControl.addJob(controlledJob1);
		jobControl.addJob(controlledJob2);

		// 启动任务
		Thread jobControlThread = new Thread(jobControl);
		jobControlThread.start();
		while (true) {
			if(jobControl.allFinished()){
				System.out.println(jobControl.getSuccessfulJobList());
				jobControl.stop();
				break;
			}
		}
	}

	static void deleteOutputFile(String path, String inputDir) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI(inputDir), conf);
		if (fs.exists(new Path(path))) {
			fs.delete(new Path(path), true);
		}
	}
}