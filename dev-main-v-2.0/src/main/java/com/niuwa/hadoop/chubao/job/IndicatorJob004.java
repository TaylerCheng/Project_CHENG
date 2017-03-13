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
			outValue.set(ChubaoUtil.array2Str(values, 1));
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
			//总呼叫数
			int sum = 0;
			//呼叫手机种类数
			int numSum=0;
			//呼叫联系人种类数
			int trueNumSum=0;

			//最大呼叫数
			int maxSum=0;
			//呼叫最大数的手机号集合
			String maxContactPhone= "";
			Boolean maxContactPhoneStatus= false;
			
			//
			List<Integer> top5Call= new ArrayList<Integer>();
			
			Comparator<Integer> comparator= new Comparator<Integer>() {
				@Override
				public int compare(Integer o1, Integer o2) {
					return o1-o2;
				}				
			};
			
			for (Text val : values) {
				String vals[] = val.toString().split("\t");
				//单个手机呼叫数
				int sum_part=Integer.parseInt(vals[1]);
				sum += sum_part;
				Boolean b = new Boolean(vals[2]);
				int call_type_1_sum= Integer.parseInt(vals[3]);
				
				// 号码记为true，排除总呼出数=1的号码
				if (b.booleanValue() && call_type_1_sum>1) {
					trueNumSum++;
				}
				// 排除总呼出数=1的号码
				if (call_type_1_sum>1) {
					numSum++;
				}
				
				if(maxSum<sum_part){
					maxSum=sum_part;
					maxContactPhone= vals[0];
					maxContactPhoneStatus= b;
				}
				if(maxSum==sum_part){
					maxContactPhoneStatus= maxContactPhoneStatus||b;
				}
				
				
				top5Call.add(sum_part);
				Collections.sort(top5Call, comparator);
				if(top5Call.size() > 5){
					top5Call.remove(0);
				}

			}
			//top5 sum
				int top5sum=0;
				for(Integer mapkey : top5Call){
					top5sum+=mapkey;
				}
				
				double call_top5_perct_type=(double)top5sum/sum;
				
				double call_true_rate_type=(double)trueNumSum/numSum;
				
				//call_true_rate_type<0.3为无效
				if(call_true_rate_type>=0.3){
					//输出结果为   userid	call_true_rate_type	 max_contact_call	call_top5_perct_type
					outObj.put("user_id", key.toString());
					outObj.put("call_true_rate_type", call_true_rate_type);
					outObj.put("call_out_6_month_sum", numSum);					
					outObj.put("call_out_true_6_month_sum", trueNumSum);
					outObj.put("max_contact_call", maxContactPhoneStatus);
					outObj.put("max_contact_call_number", maxContactPhone);
					outObj.put("call_top5_perct_type", call_top5_perct_type);
					outObj.put("total_call", sum);
					outObj.put("top5sum_call", top5sum);
					
					outValue.set(outObj.toJSONString());
					context.write(NullWritable.get(), outValue);
				}
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
