package com.niuwa.hadoop.chubao.job;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import com.niuwa.hadoop.chubao.utils.ChubaoUtil;
import com.niuwa.hadoop.util.HadoopUtil;

/**
 * input: call_log
 * output: {"user_id":"xxx","break_ratio": 0.23}
 * @author Administrator
 *
 */
public class IndicatorJob008 extends BaseJob{

	public static class UserIdsMapper extends NiuwaMapper<Object, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outValue = new Text();
		private JSONObject outObj = new JSONObject();
 		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			JSONObject json = JSON.parseObject(value.toString());
			
			String userId = json.getString("user_id");
			String otherPhone = json.getString("other_phone");
			if(ChubaoUtil.telVilidate(otherPhone) && !json.getBooleanValue("call_contact")
					&& ChubaoDateUtil.isBeforeNMonths(0, json.getLongValue("call_date")*1000)){
				outKey.set(userId);
				outObj.put("user_id", userId);
				outObj.put("call_duration", json.getIntValue("call_duration"));
				outObj.put("other_phone", json.getString("other_phone"));
				outValue.set(outObj.toJSONString());
				context.write(outKey, outValue);
			}
				
			
		}
		
	}
	
	
	public static class CallReducer extends NiuwaReducer<Text, Text, NullWritable, Text> {
		
		private Text outValue = new Text();
		private JSONObject outObj = new JSONObject();
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			Set<String> phoneSetNotIncludedInContact = new HashSet<String>();
			Set<String> breakCallsSetNotIncludedInContact = new HashSet<String>();
			
			for(Text val: values){
				JSONObject json = JSONObject.parseObject(val.toString());
				phoneSetNotIncludedInContact.add(json.getString("other_phone"));
				int callDuratoin = json.getIntValue("call_duration");
				if(0 == callDuratoin){
					breakCallsSetNotIncludedInContact.add(json.getString("other_phone"));
				}
			}
			
			int phoneNumNotIncludedInContact = phoneSetNotIncludedInContact.size();
			int breakCallsNotIncludedInContact = breakCallsSetNotIncludedInContact.size();
			
			double breakRatio = 0.0;
			if(phoneNumNotIncludedInContact > 0){
				breakRatio = breakCallsNotIncludedInContact / (phoneNumNotIncludedInContact + 0.0);
			}
			
			outObj.put("user_id", key.toString());
			if(ChubaoJobConfig.isDebugMode()){
				outObj.put("phoneNumNotIncludedInContact", phoneNumNotIncludedInContact);
				outObj.put("breakCallsNotIncludedInContact", breakCallsNotIncludedInContact);
			}
			outObj.put("break_ratio", breakRatio);
			outValue.set(outObj.toJSONString());
			context.write(NullWritable.get(), outValue);
		}
		
	}
	
	@Override
	public void setJobSpecialInfo(Job job, Configuration conf,
			RunParams params,
			Map<String, Path> tempPaths) throws Exception{
        
        job.setMapperClass(IndicatorJob008.UserIdsMapper.class);
        job.setReducerClass(IndicatorJob008.CallReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
		
        // 输入路径
        FileInputFormat.addInputPath(job, ChubaoJobConfig.getInputPath(ChubaoJobConfig.INPUT_CALL_LOG));
        // 输出路径
        FileOutputFormat.setOutputPath(job, tempPaths.get(IndicatorJob008.class.getName()) );
        // 删除原有的输出
        HadoopUtil.deleteOutputFile(tempPaths.get(IndicatorJob008.class.getName()));
        
	}
	

}
