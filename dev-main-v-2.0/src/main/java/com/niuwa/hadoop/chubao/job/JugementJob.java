package com.niuwa.hadoop.chubao.job;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Map;

import com.niuwa.hadoop.chubao.*;
import com.niuwa.hadoop.chubao.enums.RuleCounter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import com.niuwa.hadoop.chubao.rules.Rules;
import com.niuwa.hadoop.util.HadoopUtil;
/**
 * 
 * 小额指标合并过滤任务<br> 
 * 联合过滤规则和判断条件，输出最终白名单结果：
 * 输出结果：
 * user_id：用户唯一标识
 * type:1:小额，2:大额
 * amount:最终可借款金额
 * prod_rate:产品费率
 * daily_fee_rate:日费率
 * month_fee_rate:月费率（小额默认为0）
 * base_fee_rate:基本费率
 * @author maliqiang
 * @see [相关类/方法]（可选）
 * @since 2016-7-4
 */
public class JugementJob extends BaseJob {
	
    public static class MapSevralTemp extends NiuwaMapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

			JSONObject record= JSONObject.parseObject(value.toString());

			context.write(new Text(record.getString("user_id")), value);
		}
	}
	
	public static class ReduceObj extends NiuwaReducer<Text, Text, NullWritable, Text>{
		public static final Logger log = LoggerFactory.getLogger(ReduceObj.class);

		/**
		 *  触宝费率基础
		 *  {
		 *		"base_prod_rate": 0.108,
		 *		"base_daily_fee_rate": 0.005,
		 *		"base_month_fee_rate": 0.00,
		 *		"base_fee_rate": 0.01
		 *	}
		 */
		private JSONObject baseRateRecord= null;
		
		// 最终结果
 		private JSONObject finalObj= new JSONObject();
 		
		public void setup(Context context){
	        /**
	         * 读取cachefiles
	         * 
	         * 官方文档使用这个方法，测试使用上下文也能获取到，还不知道问题所在
	         * URI[] patternsURIs = Job.getInstance(context.getConfiguration()).getCacheFiles();
	         */
	        super.setup(context);
			BufferedReader reader=null;
	        try{
	        	
		        URI[] paths =context.getCacheFiles();
		        log.info("[ cached file number ]{}"+ paths.length);
		        //Path cacheFilePath= new Path(paths[1].getPath());
		        Path cacheFilePath= new Path(paths[0].getPath());
		        reader= new BufferedReader(new FileReader(cacheFilePath.getName().toString()));
		        StringBuffer content= new StringBuffer();
		        String str= null;
	            while((str= reader.readLine())!=null){
	            	content.append(str);
	            }
	            baseRateRecord= JSONObject.parseObject(content.toString());	            
	        }catch(Exception e){
	            e.printStackTrace();
	        }finally{
	            try {
					reader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        }			
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			JSONObject resultObj= new JSONObject();
			resultObj.put("user_id", key.toString());
            DecimalFormat df = new DecimalFormat("#.00000"); 
            DecimalFormat df2 = new DecimalFormat("#.00"); 
            
            initResultObj(resultObj);
            
			for(Text value : values){
				JSONObject lineObj= JSONObject.parseObject(value.toString());
				
				for(Map.Entry<String, Object> entry: lineObj.entrySet()) {
					resultObj.put(entry.getKey(), entry.getValue());
				}
			}
		
			boolean rule = JudgeRules(resultObj,context);
            //DEBUG输出符合规则和不符合规则的；非DEBUG，只输出符合规则的
            if(ChubaoJobConfig.isDebugMode() || rule){
				finalObj.put("user_id", key.toString());
				finalObj.put("type", 1);
				try {
					//可借款最终金额
					finalObj.put("amount", Double.parseDouble(df2.format(resultObj.getDouble("final_amount"))));
					finalObj.put("prod_rate",
							Double.parseDouble(df.format(baseRateRecord.getDouble("base_prod_rate"))));//APR
					finalObj.put("daily_fee_rate",
							Double.parseDouble(df.format(baseRateRecord.getDouble("daily_fee_rate"))));// 日手续费
					finalObj.put("monthly_fee_rate",
							Double.parseDouble(df.format(baseRateRecord.getDouble("base_month_fee_rate") + 0)));// 月手续费
					finalObj.put("base_fee_rate",
							Double.parseDouble(df.format(resultObj.getDouble("base_fee_rate"))));// 基本手续费
				}catch (Exception e){
					log.error("Parse prejob result error,resultObj = " + resultObj, e);
				}
                /*输出结果：
                user_id：用户唯一标识
                type:1:小额，2:大额
                amount:最终可借款金额
                prod_rate:产品费率
                daily_fee_rate:日费率
                month_fee_rate:月费率（小额默认为0）
                base_fee_rate:基本费率
                */
				context.write(NullWritable.get(), new Text(finalObj.toJSONString()));
            }
		
		}

		private boolean JudgeRules(JSONObject resultObj, Context context) {
			boolean rule1 = Rules.isMatchedRule_1(resultObj.getLong("device_activation"));
			boolean rule2 = Rules.isMatchedRule_2(resultObj.getInteger("call_num_3_month"));
			boolean rule3 = Rules.rule3(resultObj);
//			boolean rule4 = Rules.isMatchedRule4(resultObj.getInteger("total_calls_from_tel_library"),
//					resultObj.getInteger("total_diff_num_called_from_tel_library"));
			boolean rule5 = Rules.rule5(resultObj);
			boolean rule6 = Rules.rule6(resultObj.getIntValue("total_apps_from_app_library"));
			boolean rule7 = Rules.rule7(resultObj.getDoubleValue("break_ratio"));
			boolean rule8 = Rules.rule8(resultObj.getDoubleValue("good_cnt_rate"),resultObj.getDoubleValue("bad_cnt_rate"));
            boolean rule9 = Rules.rule9(resultObj.getIntValue("user_loan_overdue"));

            //规则4下线  2017/02/24
//          boolean rule = rule1 && rule2 && rule3 && rule4 && rule5 && rule6 && rule7 && rule8 && rule9;
			boolean rule = rule1 && rule2 && rule3 && rule5 && rule6 && rule7 && rule8 && rule9;

			//DEBUG模式输出规则是否命中
			if(ChubaoJobConfig.isDebugMode()) {
				if (rule) {
					context.getCounter(RuleCounter.RULE_PASS).increment(1);
				}
				if (rule1) {
					context.getCounter(RuleCounter.RULE_1).increment(1);
				}
				if (rule2) {
					context.getCounter(RuleCounter.RULE_2).increment(1);
				}
				if (rule3) {
					context.getCounter(RuleCounter.RULE_3).increment(1);
				}
				if (rule5) {
					context.getCounter(RuleCounter.RULE_5).increment(1);
				}
				if (rule6) {
					context.getCounter(RuleCounter.RULE_6).increment(1);
				}
				if (rule7) {
					context.getCounter(RuleCounter.RULE_7).increment(1);
				}
				if (rule8) {
					context.getCounter(RuleCounter.RULE_8).increment(1);
				}
				if (rule9) {
					context.getCounter(RuleCounter.RULE_9).increment(1);
				}
			    finalObj.put("rule1", rule1);
			    finalObj.put("rule2", rule2);
			    finalObj.put("rule3", resultObj.get("rule_type"));
//			    finalObj.put("rule4", rule4);
			    finalObj.put("rule5", rule5);
			    finalObj.put("rule6", rule6);
			    finalObj.put("rule7", rule7);
                finalObj.put("rule8", rule8);
                finalObj.put("rule9", rule9);

				finalObj.put("pass", rule);
            }
			return rule;
		}

		private void initResultObj(JSONObject resultObj) {
			//rule1默认值
			//激活时间默认成最大值，这样激活时间判断，不符合的肯定不过，符合的会将该值重写
            resultObj.put("device_activation", Long.MAX_VALUE);
			
			//rule2默认值
            resultObj.put("call_num_3_month", 0);
            
            //rule3默认值
            resultObj.put("contact_sum", 0);
            resultObj.put("max_contact_call", 0);
            resultObj.put("call_true_rate_type", 0.0);
            
            //rule4默认值
			//没有通话记录时，来自号码库号码拨打次数默认为0，来自不同号码数目默认为0
//            resultObj.put("total_calls_from_tel_library", 0);
//            resultObj.put("total_diff_num_called_from_tel_library", 0);
            
            //rule5默认值
            resultObj.put("m2_records", 0);
            resultObj.put("loan_max_out_day", 0);
            
            //rule6默认值
            resultObj.put("total_apps_from_app_library", 0);
            //rule7默认值
            resultObj.put("break_ratio", 0.0);
		}
	}
	
	@Override
	public void setJobSpecialInfo(Job job, Configuration conf,
			RunParams params,
			Map<String, Path> tempPaths) throws Exception{
	
		
		job.setMapperClass(JugementJob.MapSevralTemp.class);
		job.setReducerClass(JugementJob.ReduceObj.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	
		// 读取静态缓存的费率配置文件
		job.addCacheFile(ChubaoJobConfig.getConfigPath("rate-config.txt").toUri());
        // 输入路径
		FileInputFormat.addInputPath(job, tempPaths.get(IndicatorJob002.class.getName()));
		FileInputFormat.addInputPath(job, tempPaths.get(IndicatorJob003.class.getName()));
		FileInputFormat.addInputPath(job, tempPaths.get(IndicatorJob004.class.getName()));
		FileInputFormat.addInputPath(job, tempPaths.get(IndicatorJob006.class.getName()));
//		FileInputFormat.addInputPath(job, tempPaths.get(IndicatorJob0042.class.getName()));
		FileInputFormat.addInputPath(job, tempPaths.get(IndicatorJob007.class.getName()));
		FileInputFormat.addInputPath(job, tempPaths.get(IndicatorJob008.class.getName()));
		FileInputFormat.addInputPath(job, tempPaths.get(LargeIndicatorJob006.class.getName()));
		FileInputFormat.addInputPath(job, tempPaths.get(IndicatorJob010.class.getName()));

		// 输出路径
		Path outputFile= ChubaoJobConfig.getOutputPath("small");
		if(ChubaoJobConfig.isDebugMode()){
			outputFile= ChubaoJobConfig.getOutputPath("small-debug");
		}
		FileOutputFormat.setOutputPath(job, outputFile);
		// 删除原有的输出
		HadoopUtil.deleteOutputFile(outputFile);
		
	}

	@Override
	public final HashSet<String> getDependingJobNames() {
		return Sets.newHashSet(IndicatorJob002.class.getName(), IndicatorJob003.class.getName(),
				IndicatorJob004.class.getName(), IndicatorJob006.class.getName(), /*IndicatorJob0042.class.getName(),*/
				IndicatorJob007.class.getName(), IndicatorJob008.class.getName(), LargeIndicatorJob006.class.getName(),
				IndicatorJob010.class.getName());
	}
}
