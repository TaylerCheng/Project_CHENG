package com.niuwa.hadoop.chubao.job;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.alibaba.fastjson.JSONObject;
import com.niuwa.hadoop.chubao.ChubaoJobConfig;
import com.niuwa.hadoop.chubao.NiuwaMapper;
import com.niuwa.hadoop.chubao.NiuwaReducer;
import com.niuwa.hadoop.chubao.RunParams;
import com.niuwa.hadoop.util.HadoopUtil;

/**
 * 
 * 大额白名单规则1和2<br> 
 * 规则1  客户小额借款无任何逾期记录       
 * 规则2  客户至少有3笔小额借款记录   输出字段：small_records    
 * 输入来源：largeCondition1
 * 输出字段：small_records、user_id
 * @author maliqiang
 * @see 
 * @since 2016-6-24
 */
public class LargeIndicatorJob001 extends BaseJob{
    public static class LargeMapper extends NiuwaMapper<Object, Text, Text, IntWritable>{
        
        IntWritable one = new IntWritable(1);
        IntWritable zero = new IntWritable(0);
        Text outKey = new Text();
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            JSONObject loan = JSONObject.parseObject(value.toString());
            Integer loan_type = Integer.parseInt(loan.getString("loan_type"));
            String loan_repay_status = loan.getString("loan_repay_status");
            //  规则1 客户小额借款无任何逾期记录，非debug模式过滤逾期记录       
            if(isOutDays(loan_type, loan_repay_status)){
                outKey.set(loan.getString("user_id"));
                context.write(outKey, one);
            }else{
                outKey.set(loan.getString("user_id"));
                context.write(outKey, zero);
            }
        }
    }
    
   
    
    public static class LargeReducer extends NiuwaReducer<Text, IntWritable, NullWritable, Text>{
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
                //逾期的小额借款记录）       
                JSONObject result = new JSONObject();
                result.put("user_id", key.toString());
                result.put("out_days_records", sum);
                //输出结果:user_id、逾期小额借款笔数
                context.write(NullWritable.get(), new Text(result.toJSONString()));
        }
    }
    
    /**
     * 
     * 功能描述: <br>
     * 统计逾期的小额借款笔数
     *
     * @param loan_type
     * @param loan_repay_status
     * @return
     * @see [相关类/方法](可选)
     * @since [产品/模块版本](可选)
     */
    public static boolean isOutDays(int loan_type, String loan_repay_status){
        if (loan_type == 1 && (loan_repay_status.equalsIgnoreCase("o") || loan_repay_status.equalsIgnoreCase("os"))) {
            //此处是有逾期记录
            return true;
        }
        return false;
    }
    
    
    @Override
	public void setJobSpecialInfo(Job job, Configuration conf,
			RunParams params,
			Map<String, Path> tempPaths) throws Exception{
    	
        
        job.setMapperClass(LargeIndicatorJob001.LargeMapper.class);
        job.setReducerClass(LargeIndicatorJob001.LargeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        
        FileInputFormat.addInputPath(job, ChubaoJobConfig.getInputPath(ChubaoJobConfig.INPUT_LOAN));
        // 删除原有的输出
        HadoopUtil.deleteOutputFile(tempPaths.get(LargeIndicatorJob001.class.getName()));
        FileOutputFormat.setOutputPath(job, tempPaths.get(LargeIndicatorJob001.class.getName()));
        
    }
    
    
}
