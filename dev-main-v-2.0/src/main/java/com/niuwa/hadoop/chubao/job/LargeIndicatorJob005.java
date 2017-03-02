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
import com.niuwa.hadoop.chubao.rules.LargeRules;
import com.niuwa.hadoop.chubao.utils.ChubaoDateUtil;
import com.niuwa.hadoop.util.HadoopUtil;

/**
 * 
 * 大额规则3：<br>
 * 统计客户有多少笔小额优质借款记录(借款期限>20且还款期限占比>50%) 
 * 不包括逾期借款（含大小额）
 * 输入来源：loan 
 * 输出字段：superior_loans、user_id
 * 
 * @author maliqiang
 * @see
 * @since 2016-6-21
 */
public class LargeIndicatorJob005 extends BaseJob{
    public static class RuleMapper extends NiuwaMapper<Object, Text, Text, IntWritable> {
        protected void map(Object object, Text value, Context context) throws IOException, InterruptedException {
            /*
             * 此json对象包含以下字段： user_id、loan_id、credit_limit、loan_amount、loan_amount_left、loan_days、loan_rate、
             * loan_fee、loan_time、loan_end_time、loan_repay_status、loan_check_status、loan_out_day、
             * loan_max_out_day、loan_out_amount、loan_max_out_amount、loan_type
             */
            JSONObject loan = JSONObject.parseObject(value.toString());

            IntWritable one = new IntWritable(1);
            IntWritable zero = new IntWritable(0);

            int loan_days = loan.getInteger("loan_days");// 借款天数
            String loan_repay_status = loan.getString("loan_repay_status");//还款状态
            String loan_check_status = loan.getString("loan_check_status");//审核状态

            int loan_type = loan.getInteger("loan_type");//借款类型为小额
            double repayRate = getRepayRate(loan);// 还款期限占比
            // 满足大额规则3的借款笔数
            //S审核通过 S已结清 loan_type=1（小额）
            if (loan_type==1&&loan_check_status.equalsIgnoreCase("s")&&loan_repay_status.equalsIgnoreCase("s")&&LargeRules.largeRule3_param(loan_days, repayRate)) {
                context.write(new Text(loan.getString("user_id")), one);
            }

        }

    }

    public static class RuleReducer extends NiuwaReducer<Text, IntWritable, NullWritable, Text> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {
            int sum = 0;
            JSONObject resultObject = new JSONObject();
            for (IntWritable value : values) {
                sum += value.get();
            }
            
            // 输出结果：user_id、superior_loans
            resultObject.put("superior_loans", sum);
            resultObject.put("user_id", key.toString());
            context.write(NullWritable.get(),new Text(resultObject.toJSONString()));

        }
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
        if ("s".equalsIgnoreCase(loan.getString("loan_repay_status"))
                ) {
            int loanDays = loan.getInteger("loan_days");// 贷款期限
            long loanTime = loan.getInteger("loan_time");// 贷款时间
            long finishRepayTime = loan.getLong("loan_end_time");// 还款完成时间
            /**
             * 还款期限占比 = 实际借款时间/计划借款期限
             */
            double repayRate = 0.0;
            long realLoanDays = ChubaoDateUtil.getDateInterval(finishRepayTime, loanTime);
            if (realLoanDays == 0) {
                repayRate = 1;
            } else {
                repayRate = (double) realLoanDays / loanDays;// 实际借款时间/协议(计划)借款时间
            }
            return repayRate;
        } else {
            return 0;
        }

    }
    
    @Override
	public void setJobSpecialInfo(Job job, Configuration conf,
			RunParams params,
			Map<String, Path> tempPaths) throws Exception{
        
        job.setMapperClass(LargeIndicatorJob005.RuleMapper.class);
        job.setReducerClass(LargeIndicatorJob005.RuleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        
        FileInputFormat.addInputPath(job, ChubaoJobConfig.getInputPath(ChubaoJobConfig.INPUT_LOAN));
        // 删除原有的输出
        HadoopUtil.deleteOutputFile(tempPaths.get(LargeIndicatorJob005.class.getName()));
        FileOutputFormat.setOutputPath(job, tempPaths.get(LargeIndicatorJob005.class.getName()));
        
    }

}
