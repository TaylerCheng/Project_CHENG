package com.niuwa.hadoop.chubao.job;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import com.niuwa.hadoop.chubao.ChubaoJobConfig;
import com.niuwa.hadoop.chubao.NiuwaMapper;
import com.niuwa.hadoop.chubao.NiuwaReducer;
import com.niuwa.hadoop.chubao.RunParams;
import com.niuwa.hadoop.chubao.rules.LargeRules;
import com.niuwa.hadoop.chubao.rules.Rules;
import com.niuwa.hadoop.util.HadoopUtil;

/**
 * 
 * 大额定额<br>
 * 根据参数确定用户可借的最终额度 输出结果： user_id：用户唯一标识 type:1:小额，2:大额 amount:最终可借款金额 prod_rate:产品费率 daily_fee_rate:日费率
 * month_fee_rate:月费率（小额默认为0） base_fee_rate:基本费率
 * 
 * @author maliqiang
 * @see [相关类/方法]（可选）
 * @since [产品/模块版本] （可选）
 */
public class LargeJugementJob extends BaseJob {
	
	private final HashSet<String> dependingJobsName = Sets.newHashSet(LargeIndicatorJob002.class.getName(),
			LargeIndicatorJob004.class.getName(), LargeIndicatorJob005.class.getName(), LargeIndicatorJob006.class.getName());

    public static class MapSevralTemp extends NiuwaMapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            JSONObject record = JSONObject.parseObject(value.toString());
            context.write(new Text(record.getString("user_id")), value);
        }
    }

    public static class ReduceObj extends NiuwaReducer<Text, Text, NullWritable, Text> {
        JSONObject finalObj = new JSONObject();
        Logger log = Logger.getLogger(ReduceObj.class);
        private JSONObject baseRateRecord;

        public void setup(Context context) {
            /**
             * 读取cachefiles
             * 
             * 官方文档使用这个方法，测试使用上下文也能获取到，还不知道问题所在 URI[] patternsURIs =
             * Job.getInstance(context.getConfiguration()).getCacheFiles();
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

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            JSONObject resultObj = new JSONObject();
            finalObj.put("user_id", key.toString());
            for (Text value : values) {
                JSONObject lineObj = JSONObject.parseObject(value.toString());

                if (lineObj.get("amt") != null) {
                    resultObj.put("amt", lineObj.get("amt"));
                }
                if (lineObj.get("loan_type") != null && lineObj.getInteger("loan_type") == 2) {
                    resultObj.put("type", lineObj.get("loan_type"));// 大额贷款
                }
                // 大额规则3
                if (lineObj.get("loan_days") != null) {
                    resultObj.put("loan_days", lineObj.get("loan_days"));
                }
                if (lineObj.get("repay_rate") != null) {
                    resultObj.put("repay_rate", lineObj.get("repay_rate"));
                }
                if (lineObj.get("loan_amount") != null) {
                    resultObj.put("loan_amount", lineObj.get("loan_amount"));
                }
                if (lineObj.get("small_records") != null) {
                    resultObj.put("small_records", lineObj.get("small_records"));
                }

                if (lineObj.get("loan_max_out_day") != null) {
                    resultObj.put("loan_max_out_day", lineObj.get("loan_max_out_day"));
                }
                if (lineObj.get("final_amount") != null) {
                    resultObj.put("final_amount", lineObj.get("final_amount"));
                }
                if (lineObj.get("out_days_records") != null) {
                    resultObj.put("out_days_records", lineObj.get("out_days_records"));
                }
                if (lineObj.get("superior_loans") != null) {
                    resultObj.put("superior_loans", lineObj.get("superior_loans"));
                }
                if (lineObj.get("m2_records") != null) {
                    resultObj.put("m2_records", lineObj.get("m2_records"));
                }
                
            }
           
            // rules
            if (Rules.joinRule(resultObj, "amt", "small_records", "final_amount", "out_days_records","superior_loans","m2_records")) {

                boolean rule1 = LargeRules.largeRule1(resultObj.getInteger("out_days_records"))&&resultObj.getInteger("m2_records")==0;//未进入M2
                boolean rule2 = LargeRules.largeRule2(resultObj.getInteger("small_records"));
                int superior_loans = resultObj.getInteger("superior_loans");
                boolean rule3 = LargeRules.largeRule3(superior_loans);// 规则3：至少一笔优质借款记录
                boolean rule = rule1 && rule2 && rule3;
                // DEBUG模式输出规则命中情况（rule1为没有逾期的小额借款记录，结果必命中）
                if (ChubaoJobConfig.isDebugMode()) {
                    finalObj.put("rule1", rule1);
                    finalObj.put("rule2", rule2);
                    finalObj.put("rule3", rule3);
                }

                /*
                 * 输出结果： user_id：用户唯一标识 type:1:小额，2:大额 amount:最终可借款金额 prod_rate:产品费率 daily_fee_rate:日费率
                 * month_fee_rate:月费率（小额默认为0） base_fee_rate:基本费率
                 */
                DecimalFormat df = new DecimalFormat("#.00000");
                if (ChubaoJobConfig.isDebugMode()||rule) {
                    finalObj.put("type", 2);
                    finalObj.put("amount", Double.parseDouble(df.format(resultObj.getDouble("final_amount"))));
                    finalObj.put("prod_rate", Double.parseDouble(df.format(baseRateRecord.getDouble("base_prod_rate"))));// APR
                    finalObj.put("daily_fee_rate",
                            Double.parseDouble(df.format(baseRateRecord.getDouble("base_daily_fee_rate"))));// 日手续费
                    finalObj.put("monthly_fee_rate",
                            Double.parseDouble(df.format(baseRateRecord.getDouble("base_month_fee_rate"))));// 月手续费
                    finalObj.put("base_fee_rate",
                            Double.parseDouble(df.format(baseRateRecord.getDouble("base_fee_rate"))));// 基本手续费
                        context.write(NullWritable.get(), new Text(finalObj.toJSONString()));
                }
            }
        }
    }
    
    @Override
    public void setJobSpecialInfo(Job job, Configuration conf,
			RunParams params,
			Map<String, Path> tempPaths) throws Exception{      
        
        job.setMapperClass(LargeJugementJob.MapSevralTemp.class);
        job.setReducerClass(LargeJugementJob.ReduceObj.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.addCacheFile(ChubaoJobConfig.getConfigPath("rate-config-large.txt").toUri());
        // 输入路径
        FileInputFormat.addInputPath(job, tempPaths.get(LargeIndicatorJob001.class.getName()));
        FileInputFormat.addInputPath(job, tempPaths.get(LargeIndicatorJob002.class.getName()));
        FileInputFormat.addInputPath(job, tempPaths.get(LargeIndicatorJob003.class.getName()));
        FileInputFormat.addInputPath(job, tempPaths.get(LargeIndicatorJob004.class.getName()));
        FileInputFormat.addInputPath(job, tempPaths.get(LargeIndicatorJob005.class.getName()));
        FileInputFormat.addInputPath(job, tempPaths.get(LargeIndicatorJob006.class.getName()));
        // 输出路径
		Path outputFile= ChubaoJobConfig.getOutputPath("large");
		if(ChubaoJobConfig.isDebugMode()){
			outputFile= ChubaoJobConfig.getOutputPath("large-debug");
		}        
        FileOutputFormat.setOutputPath(job, outputFile);
        // 删除原有的输出
        HadoopUtil.deleteOutputFile(outputFile);
        
    }

    @Override
	public final HashSet<String> getDependingJobNames(){
		return dependingJobsName;
	}
}
