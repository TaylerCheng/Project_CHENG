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
 * 规则1条件：大额未进入M2  
 * 输入来源：loan
 * 输出字段：m2_records（大额进入M2的记录）、user_id
 * @author maliqiang
 * @see 
 * @since 2016-7-27
 */
public class LargeIndicatorJob006 extends BaseJob{
    public static class LargeMapper extends NiuwaMapper<Object, Text, Text, IntWritable>{
        
        IntWritable one = new IntWritable(1);
        IntWritable zero = new IntWritable(0);
        Text outKey = new Text();
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            JSONObject loan = JSONObject.parseObject(value.toString());
            int type = loan.getInteger("loan_type");
            //  规则1 客户小额借款无任何逾期记录，非debug模式过滤逾期记录    
            if(type==2){
                if(isEntryM2(loan)){
                    outKey.set(loan.getString("user_id"));
                    context.write(outKey, one);
                }
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
                result.put("m2_records", sum);
                //输出结果:user_id、逾期小额借款笔数
                context.write(NullWritable.get(), new Text(result.toJSONString()));
        }
    }
    /**
     * 
     * 功能描述: <br>
     * 每笔已结清借款未进入过M2
     *
     * @param lineObj
     * @return
     */
    public static boolean isEntryM2(JSONObject lineObj) {
        if (lineObj.get("loan_max_out_day") != null
                && lineObj.getInteger("loan_max_out_day") >= 31) {
            return true;
        }
        return false;
    }
    
    @Override
    public void setJobSpecialInfo(Job job, Configuration conf,
			RunParams params,
			Map<String, Path> tempPaths) throws Exception{
    	
        job.setMapperClass(LargeIndicatorJob006.LargeMapper.class);
        job.setReducerClass(LargeIndicatorJob006.LargeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        

        FileInputFormat.addInputPath(job, ChubaoJobConfig.getInputPath(ChubaoJobConfig.INPUT_LOAN));
        // 删除原有的输出
        FileOutputFormat.setOutputPath(job, tempPaths.get(LargeIndicatorJob006.class.getName()));
        HadoopUtil.deleteOutputFile(tempPaths.get(LargeIndicatorJob006.class.getName()));
        
    }
    
}
