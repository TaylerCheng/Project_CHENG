package com.niuwa.hadoop.chubao.job;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.alibaba.fastjson.JSONObject;
import com.niuwa.hadoop.chubao.ChubaoJobConfig;
import com.niuwa.hadoop.chubao.NiuwaMapper;
import com.niuwa.hadoop.chubao.NiuwaReducer;
import com.niuwa.hadoop.chubao.RunParams;
import com.niuwa.hadoop.chubao.rules.Rules;
import com.niuwa.hadoop.util.HadoopUtil;
/**
 * 
 * call_log 计算出相关指标：<br>
 * 
 * [job1]
 * map:		UserIdAndOtherPhoneMapper
 * reduce: 	SumReducer
 * input: 	通话记录
 * output[format 自定义]：user_id	other_phone	{call_number} call_contact call_type_1_sum
 * 
 *
 * @author maliqiang
 */
public class IndicatorJob001 extends BaseJob{
	
	public static class UserIdAndOtherPhoneMapper extends
			NiuwaMapper<Object, Text, Text, Text> {

		private Text outKey = new Text();
		private Text outValue = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			JSONObject form = JSONObject.parseObject(value.toString());

			if (Rules.callLogMatchRule_3(form) && (ChubaoJobConfig.isDebugMode() || Rules
					.isMatchedRule_1(form.getLong("device_activation")))) {
				outKey.set(form.getString("user_id") + "\t" + form.getString("other_phone"));
				outValue.set(1 + "\t" + form.getBooleanValue("call_contact"));
				context.write(outKey, outValue);
			}
		}
	}

	public static class SumByUserIdAndOtherPhoneReducer extends NiuwaReducer<Text, Text, Text, Text> {
		private Text outValue = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int call_type_1_sum = 0;
			boolean callContactIsTrue = false;
			for (Text val : values) {
				String[] strs = val.toString().split("\t");
				call_type_1_sum +=Integer.valueOf(strs[0]);
				Boolean b = new Boolean(strs[1]);
				callContactIsTrue = callContactIsTrue || b.booleanValue();
			}
			outValue.set(call_type_1_sum + "\t" + callContactIsTrue);
			context.write(key, outValue);
		}
	}
	
	@Override
	public void setJobSpecialInfo(Job job, Configuration conf,
			RunParams params,
			Map<String, Path> tempPaths) throws Exception{
		
		job.setMapperClass(IndicatorJob001.UserIdAndOtherPhoneMapper.class);
		job.setCombinerClass(IndicatorJob001.SumByUserIdAndOtherPhoneReducer.class);
		job.setReducerClass(IndicatorJob001.SumByUserIdAndOtherPhoneReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// 输入路径
		FileInputFormat.addInputPath(job, ChubaoJobConfig.getInputPath(ChubaoJobConfig.INPUT_CALL_LOG));
		// 输出路径
		FileOutputFormat.setOutputPath(job, tempPaths.get(IndicatorJob001.class.getName()));
		// 删除原有的输出
		HadoopUtil.deleteOutputFile(tempPaths.get(IndicatorJob001.class.getName()));
		
	}


}