package com.niuwa.hadoop.chubao.job;

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
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.alibaba.fastjson.JSONObject;
import com.niuwa.hadoop.chubao.ChubaoJobConfig;
import com.niuwa.hadoop.chubao.RunParams;
import com.niuwa.hadoop.chubao.rules.Rules;
import com.niuwa.hadoop.util.HadoopUtil;
/**
 * 
 * 计算联系人总数<br> 
 * 输入来源：contact
 * 输出字段：contact_sum、user_id
 * @author maliqiang
 * @since 2016-7-4 
 */
public class IndicatorJob002 extends BaseJob {
	public static class UserIdMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		private Text outKey = new Text();  
		private final static IntWritable one = new IntWritable(1);
		Date now=new Date();
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			JSONObject form = JSONObject.parseObject(value.toString());
			//condition1 客户激活时间距当前不少于3个月
			if(ChubaoJobConfig.isDebugMode() 
					||(Rules.isMatchedRule_1(form.getLong("device_activation")))){
				outKey.set(form.getString("user_id"));
				context.write(outKey, one);
			}
		}
	}
	
	public static class CombinerSumReducer extends
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

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, NullWritable, Text> {
		private JSONObject result= new JSONObject(); 
		
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			//输出结果：user_id、联系人总数
			result.put("user_id", key.toString());
			result.put("contact_sum", sum);
			
			context.write(NullWritable.get(), new Text(result.toJSONString()));
		}
	}
	
	@Override
	public void setJobSpecialInfo(Job job, Configuration conf,
			RunParams params,
			Map<String, Path> tempPaths) throws Exception{
		
		job.setMapperClass(IndicatorJob002.UserIdMapper.class);
		job.setCombinerClass(IndicatorJob002.CombinerSumReducer.class);
		job.setReducerClass(IndicatorJob002.IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		
		// 输入路径
		FileInputFormat.addInputPath(job, ChubaoJobConfig.getInputPath(ChubaoJobConfig.INPUT_CONTACT));
		// 输出路径
		FileOutputFormat.setOutputPath(job, tempPaths.get(IndicatorJob002.class.getName()) );
		// 删除原有的输出
		HadoopUtil.deleteOutputFile(tempPaths.get(IndicatorJob002.class.getName()));
		
	}

}