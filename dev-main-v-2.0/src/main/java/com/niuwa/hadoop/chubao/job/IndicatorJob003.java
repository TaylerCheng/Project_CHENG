package com.niuwa.hadoop.chubao.job;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 * [job1]
 * map:		CallLogMapper
 * combiner:IntCombiner
 * reduce: 	IntSumReducer
 * input: 	通话记录
 * output[format json]：
 * 		user_id	
 * 		call_num_3_month 客户最近三个月通话记录数
 * 
 * @author Administrator
 *
 */
/*
输入数据来源：
call_log

输出数据结构
{
	"call_num_3_month": 240,
	"user_id": "e3c8634b963163bace16322f31c4a990"
}
 */
public class IndicatorJob003 extends BaseJob{
	public static class UserIdMapper extends
			NiuwaMapper<Object, Text, Text, IntWritable> {
		private Text outKey = new Text();  
		private final static IntWritable one = new IntWritable(1);
		
		Date now=new Date();
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			JSONObject form = JSONObject.parseObject(value.toString());

			// 过滤统计最近三个月的通话记录
			if( Rules.isMatchedRule_2_1(form.getLong("call_date"))
					&&(ChubaoJobConfig.isDebugMode()
							|| Rules.isMatchedRule_1(form.getLong("device_activation")))
				){
			
				outKey.set(form.getString("user_id"));
				context.write(outKey, one);
			}
		}
	}

	public static class IntCombiner extends NiuwaReducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable totalSum= new IntWritable(0);
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			totalSum.set(sum);
			
			context.write(key, totalSum);
		}
	}
	
	public static class IntSumReducer extends NiuwaReducer<Text, IntWritable, Text, Text> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			if(ChubaoJobConfig.isDebugMode() 
					||(Rules.isMatchedRule_2(sum))){
				JSONObject result= new JSONObject();
				result.put("user_id", key.toString());
				result.put("call_num_3_month", sum);
				context.write(null, new Text(result.toJSONString()));
			}
		}
	}
	
	@Override
	public void setJobSpecialInfo(Job job, Configuration conf,
			RunParams params,
			Map<String, Path> tempPaths) throws Exception{
	
		job.setMapperClass(IndicatorJob003.UserIdMapper.class);
		job.setCombinerClass(IndicatorJob003.IntCombiner.class);
		job.setReducerClass(IndicatorJob003.IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		
		// 输入路径
		FileInputFormat.addInputPath(job, ChubaoJobConfig.getInputPath(ChubaoJobConfig.INPUT_CALL_LOG));
		// 输出路径
		FileOutputFormat.setOutputPath(job, tempPaths.get(IndicatorJob003.class.getName()) );
		// 删除原有的输出
		HadoopUtil.deleteOutputFile(tempPaths.get(IndicatorJob003.class.getName()));
		
	}

}