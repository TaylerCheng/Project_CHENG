package com.niuwa.hadoop.chubao.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import com.niuwa.hadoop.chubao.NiuwaMapper;
import com.niuwa.hadoop.chubao.NiuwaReducer;
import com.niuwa.hadoop.chubao.RunParams;
import com.niuwa.hadoop.chubao.utils.ChubaoUtil;
import com.niuwa.hadoop.util.HadoopUtil;

/**
 * 
 * map:		UserIdMapper
 * reduce: 	Sum1Reducer
 * input: 	job1的输出
 * output[format json]：
 *		user_id：用户唯一标识
 *		call_true_rate_type：呼出电话中标记为true的号码数占呼出总号码数的占比(若某号码曾经被标记为true, 则该号码记为true)
 *		max_contact_call：最频繁呼出号码是否标记为true
 *		call_top5_perct_type：最频繁呼出前5号码总呼出数占总呼出数占比
 *
 * @author Administrator
 *
 */
public class IndicatorJob004 extends BaseJob {
	
	private final HashSet<String> dependingJobsName = Sets.newHashSet(IndicatorJob001.class.getName());
	
	/**
	 *  按照user_id 统计
	 *  
	 * @author Administrator
	 *
	 */
	public static class UserIdMapper extends NiuwaMapper<Object, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outValue = new Text();
		Date now = new Date();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] values=value.toString().split("\t");
			outKey.set(values[0]);
			outValue.set(value);
			context.write(outKey, outValue);
		}
	}

	/**
	 * 
	 * @author yanpeng
	 *
	 */	
	public static class Sum1Reducer extends NiuwaReducer<Text, Text, NullWritable, Text> {
		private Text outValue = new Text();
		private JSONObject outObj= new JSONObject();
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//呼出总号码数
			int call_out_6_month_sum = 0;
			//呼出电话中标记为true的号码数
			int call_out_true_6_month_sum = 0;
			for (Text val : values) {
				// value中的字段为{user_id，other_phone，call_type_1_sum, callContactIsTrue}
				String vals[] = val.toString().split("\t");
				//用户与单个手机呼出通话数
				int call_type_1_sum = Integer.parseInt(vals[2]);
				// 排除总呼出数=1的号码
				if (call_type_1_sum > 1) {
					call_out_6_month_sum++;
					// 统计号码标记为true
					Boolean callContactIsTrue = new Boolean(vals[3]);
					if (callContactIsTrue.booleanValue()) {
						call_out_true_6_month_sum++;
					}
				}

			}

			double call_true_rate_type = (double) call_out_true_6_month_sum / call_out_6_month_sum;

            outObj.put("user_id", key.toString());
            outObj.put("call_true_rate_type", call_true_rate_type);
            outObj.put("call_out_6_month_sum", call_out_6_month_sum);
            outObj.put("call_out_true_6_month_sum", call_out_true_6_month_sum);

			outValue.set(outObj.toJSONString());
			context.write(NullWritable.get(), outValue);
		}
	}
	
	@Override
	public void setJobSpecialInfo(Job job, Configuration conf,
			RunParams params,
			Map<String, Path> tempPaths) throws Exception{
		
		job.setMapperClass(IndicatorJob004.UserIdMapper.class);
		job.setReducerClass(IndicatorJob004.Sum1Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// 输入路径
		FileInputFormat.addInputPath(job, tempPaths.get(IndicatorJob001.class.getName()) );
		// 输出路径
		FileOutputFormat.setOutputPath(job, tempPaths.get(IndicatorJob004.class.getName()) );
		// 删除原有的输出
		HadoopUtil.deleteOutputFile(tempPaths.get(IndicatorJob004.class.getName()));
		
	}
	
	@Override
	public final HashSet<String> getDependingJobNames(){
		return dependingJobsName;
	}
}
