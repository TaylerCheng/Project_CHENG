package com.niuwa.hadoop.chubao.job;

import java.io.IOException;
import java.util.HashSet;
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
import com.niuwa.hadoop.chubao.ChubaoJobConfig;
import com.niuwa.hadoop.chubao.NiuwaMapper;
import com.niuwa.hadoop.chubao.NiuwaReducer;
import com.niuwa.hadoop.chubao.RunParams;
import com.niuwa.hadoop.chubao.rules.Rules;
import com.niuwa.hadoop.chubao.utils.ChubaoDateUtil;
import com.niuwa.hadoop.chubao.utils.ChubaoUtil;
import com.niuwa.hadoop.util.HadoopUtil;

/**
 * 
 * 大额定额任务：<br>
 * 根据用户是否逾期、还款期限占比等计算出可借款基数 
 * 输入来源：loan、large_rank_addr 
 * 输出字段：user_id、loan_max_out_day、final_amount
 * 
 * @author maliqiang
 * @see
 * @since 2016-6-21
 */
public class LargeIndicatorJob002 extends BaseJob{
	
	private final HashSet<String> dependingJobsName = Sets.newHashSet(LargeIndicatorJob001.class.getName(),LargeIndicatorJob003.class.getName());
	
    public static class RationMapper extends NiuwaMapper<Object, Text, Text, Text> {
        protected void map(Object object, Text value, Context context) throws IOException, InterruptedException {
            /*
             * 此json对象包含以下字段： user_id、loan_id、credit_limit、loan_amount、loan_amount_left、loan_days、loan_rate、
             * loan_fee、loan_time、loan_end_time、loan_repay_status、loan_check_status、loan_out_day、
             * loan_max_out_day、loan_out_amount、loan_max_out_amount、loan_type 确定初始金额文件包含字段：amt、user_id
             */
            JSONObject loan = JSONObject.parseObject(value.toString());
            JSONObject resultObj = new JSONObject();
            if (loan.get("loan_type") == null) {
                context.write(new Text(loan.getString("user_id")), value);
            } else if (loan.getInteger("loan_type") == 2) {

                // 输出以下字段
                resultObj.put("user_id", loan.getString("user_id"));// 用户标识
                resultObj.put("loan_amount", loan.getString("loan_amount"));// 借款金额
                resultObj.put("loan_out_day", loan.getInteger("loan_out_day"));// 逾期天数
                resultObj.put("loan_max_out_day", loan.getInteger("loan_max_out_day"));// 最大逾期天数
                resultObj.put("loan_repay_status", loan.getString("loan_repay_status"));// 还款状态
                double repayRate = getRepayRate(loan);//还款时间占比
                resultObj.put("repay_increate", getIncrese(loan, repayRate));// 每笔增长额度
                context.write(new Text(loan.getString("user_id")), new Text(resultObj.toJSONString()));
            }
        }
    }

    public static class RationReducer extends NiuwaReducer<Text, Text, NullWritable, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            JSONObject result = new JSONObject();
            double sum = 0.0;
            for (Text value : values) {
               
                JSONObject object = JSONObject.parseObject(value.toString());
                if (object.getDouble("repay_increate") != null) {
                    sum += object.getDouble("repay_increate");
                }
                
                if (object.getInteger("loan_days") != null) {
                    result.put("loan_days", object.getInteger("loan_days"));
                    result.put("daily_fee_rate", object.getDouble("daily_fee_rate"));
                }
                if (object.get("amt") != null) {
                    result.put("amt", object.getDouble("amt"));
                }
                if (object.getInteger("repay_rate") != null) {
                    result.put("repay_rate", object.getDouble("repay_rate"));
                }

            }

            if (Rules.joinRule(result, "amt")) {
                // 增长额+初始额度
                double finalAmount = Math.min(sum + result.getDouble("amt"), 10000);
                result.put("final_amount", ChubaoUtil.getIntAmount(finalAmount));
                result.put("user_id", key.toString());
                context.write(NullWritable.get(), new Text(result.toJSONString()));
            }

        }
    }

    private static double getIncrese(JSONObject loan,  double repayRate) {
        int loanDays = loan.getInteger("loan_days");//此处是大额的借款期数
        double repayFactor = getRepayFactor(loanDays, repayRate);//还款系数：借款时间和还款时间占比决定
        Double incrementAmount = Math.min(loan.getDouble("loan_amount") * repayFactor, 2000);
        return incrementAmount;
    }

    /**
     * 
     * 功能描述: <br>
     * 获取还款系数
     *
     * @param loanDays
     * @param repayRate
     * @return
     * @see [相关类/方法](可选)
     * @since 2016-6-24
     */
    public static double getRepayFactor(int loanDays, double repayRate) {
        double repayFactor = 0;
        if (loanDays <= 6) {
            if (repayRate <= 0.5) {
                repayFactor = 0.25;
                return repayFactor;
            }
            repayFactor = 0.5;
            return repayFactor;
        }
        if (repayRate <= 0.5) {
            repayFactor = 0.5;
            return repayFactor;
        }
        repayFactor = 1;
        return repayFactor;
    }


    /**
     * 
     * 功能描述: <br>
     * 计算正常还款额度增加值
     *
     * @param loan
     * @return
     * @see 正常还款：状态已结清（s）
     * @since [产品/模块版本](可选)
     */
    private static double getRepayRate(JSONObject loan) {
        // 贷款期限，大额需要计算，不能直接拿loan_days,大额loan_days代表期数
        int loanDays = ChubaoDateUtil.getDateInterval(loan.getInteger("loan_expire_date"), loan.getInteger("loan_time"));
        long loanTime = loan.getInteger("loan_time");// 贷款时间
        long finishRepayTime = loan.getLong("loan_end_time");// 还款完成时间
        /**
         * 还款期限占比 = 实际借款时间/计划借款期限
         */
        double repayRate = 0.0;
        int realLoanDays = ChubaoDateUtil.getDateInterval(finishRepayTime, loanTime);
        if (realLoanDays == 0) {
            repayRate = 1;
        } else {
            repayRate = (double) realLoanDays / loanDays;// 还款期限占比
        }
        return repayRate;
    }
    
    @Override
	public void setJobSpecialInfo(Job job, Configuration conf,
			RunParams params,
			Map<String, Path> tempPaths) throws Exception{
    	
        
        job.setMapperClass(LargeIndicatorJob002.RationMapper.class);
        job.setReducerClass(LargeIndicatorJob002.RationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        
        // 输入路径
        FileInputFormat.addInputPath(job, ChubaoJobConfig.getInputPath(ChubaoJobConfig.INPUT_LOAN));
        FileInputFormat.addInputPath(job, tempPaths.get(LargeIndicatorJob003.class.getName()));
        // 输出路径
        FileOutputFormat.setOutputPath(job, tempPaths.get(LargeIndicatorJob002.class.getName()));
        // 删除原有的输出
        HadoopUtil.deleteOutputFile(ChubaoJobConfig.getTempPath(LargeIndicatorJob002.class.getName()));
        
    }
    
    @Override
	public final HashSet<String> getDependingJobNames(){
		return dependingJobsName;
	}

}
