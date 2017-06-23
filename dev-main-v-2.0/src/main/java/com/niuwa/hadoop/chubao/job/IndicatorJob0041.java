package com.niuwa.hadoop.chubao.job;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.niuwa.hadoop.chubao.ChubaoJobConfig;
import com.niuwa.hadoop.chubao.NiuwaMapper;
import com.niuwa.hadoop.chubao.NiuwaReducer;
import com.niuwa.hadoop.chubao.RunParams;
import com.niuwa.hadoop.chubao.utils.ChubaoDateUtil;
import com.niuwa.hadoop.util.HadoopUtil;

/** 读取call_log
 *  输出{"user_id": "xxx", "other_phone": "xxx", "total_call_num": 123}
 * @author Administrator
 * @deprecated 规则4下线 since 2017/03/15
 */
@Deprecated
public class IndicatorJob0041 extends BaseJob{
	
	public static class UserIdsMapper extends NiuwaMapper<Object, Text, Text, IntWritable> {
		private Text outKey = new Text();
		
		private final static IntWritable one = new IntWritable(1);
 		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			JSONObject callLogJson = JSON.parseObject(value.toString());
			
			String otherPhone = callLogJson.getString("other_phone");
			String userId = callLogJson.getString("user_id");
			
			if(isCalled(callLogJson) && isCallMadeInRecentOneYear(callLogJson)
					&& !StringUtils.isEmpty(otherPhone)){
				outKey.set(userId + "\t" + otherPhone);
				context.write(outKey, one);
			}
			
		}

		private boolean isCalled(JSONObject callLogJson) {
			return callLogJson.getInteger("call_type") == 0;
		}
		
		private boolean isCallMadeInRecentOneYear(JSONObject callLogJson) {
			int monthsPerYear = 12;
			return ChubaoDateUtil.isInRecentNMonthsWithBothEndsIncluded(monthsPerYear, 
					callLogJson.getLongValue("call_date")*1000);
		}
		
	}
	
	public static class IntCombiner extends NiuwaReducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable totalSum = new IntWritable(0);
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable val : values){
				sum += val.get();
			}
			totalSum.set(sum);
			
			context.write(key, totalSum);
		}
	}
	
	public static class SumCallsReducer extends NiuwaReducer<Text, IntWritable, NullWritable, Text> {
		
		private Text outValue = new Text();
		private JSONObject outObj = new JSONObject();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable val : values){
				sum += val.get();
			}
			
			String[] keys = key.toString().split("\t");
			outObj.put("user_id", keys[0]);
			
			outObj.put("other_phone", keys[1]);
			outObj.put("total_call_num", sum);
			outValue.set(outObj.toJSONString());
			context.write(NullWritable.get(), outValue);
		}
	}
	
	@Override
	public void setJobSpecialInfo(Job job, Configuration conf,
			RunParams params,
			Map<String, Path> tempPaths) throws Exception{
		
		
		job.setMapperClass(IndicatorJob0041.UserIdsMapper.class);
		job.setCombinerClass(IndicatorJob0041.IntCombiner.class);
		job.setReducerClass(IndicatorJob0041.SumCallsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		// 输入路径
		FileInputFormat.addInputPath(job, ChubaoJobConfig.getInputPath(ChubaoJobConfig.INPUT_CALL_LOG));
		// 输出路径
		FileOutputFormat.setOutputPath(job, tempPaths.get(IndicatorJob0041.class.getName()) );
		// 删除原有的输出
		HadoopUtil.deleteOutputFile( tempPaths.get(IndicatorJob0041.class.getName()));
		
	}

}
