package com.niuwa.hadoop.chubao.job;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import com.niuwa.hadoop.chubao.ChubaoJobConfig;
import com.niuwa.hadoop.chubao.NiuwaMapper;
import com.niuwa.hadoop.chubao.NiuwaReducer;
import com.niuwa.hadoop.chubao.RunParams;
import com.niuwa.hadoop.chubao.constant.ChubaoConstants;
import com.niuwa.hadoop.chubao.rules.Rules;
import com.niuwa.hadoop.chubao.utils.ChubaoDateUtil;
import com.niuwa.hadoop.chubao.utils.ChubaoUtil;
import com.niuwa.hadoop.util.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

/**
 * 
 * 定额任务：<br>
 * 根据用户是否逾期、还款期限占比等计算出可借款基数 输入来源：loan、rank_addr 输出字段：final_amount、user_id、loan_max_out_day
 * 
 * @author maliqiang
 * @see
 * @since 2016-6-21
 */
public class NewIndicatorJob006 extends BaseJob {

    public static class RationMapper extends NiuwaMapper<Object, Text, Text, Text> {
        protected void map(Object object, Text value, Context context) throws IOException, InterruptedException {
            /*
             * 此json对象包含以下字段： user_id、loan_id、credit_limit、loan_amount、loan_amount_left、loan_days、loan_rate、
             * loan_fee、loan_time、loan_end_time、loan_repay_status、loan_check_status、loan_out_day、
             * loan_max_out_day、loan_out_amount、loan_max_out_amount、loan_type 确定初始金额文件包含字段：amt、daily_fee_rate、user_id
             */
            JSONObject loan = JSONObject.parseObject(value.toString());
            JSONObject resultObj = new JSONObject();
            if (loan.get("loan_type") == null) {
                context.write(new Text(loan.getString("user_id")), value);
            } else if (loan.getInteger("loan_type") == 1) {
                // 输出以下字段
                resultObj.put("user_id", loan.getString("user_id"));
                resultObj.put("loan_out_day", loan.getInteger("loan_out_day"));// 逾期天数,小额没有最大逾期记录，所以比较该值取最大
                resultObj.put("loan_repay_status", loan.getString("loan_repay_status"));// 还款状态
                resultObj.put("loan_check_status", loan.getString("loan_check_status"));// 审核状态
                resultObj.put("repay_late_increate_rate", getRate(loan.getInteger("loan_out_day")));// 每笔增长额度(逾期)
                resultObj.put("repay_nomarl_increate", getIncreseNormal(loan));// 每笔增长额度（正常）
                context.write(new Text(loan.getString("user_id")), new Text(resultObj.toJSONString()));
            }
        }

    }

    public static class RationReducer extends NiuwaReducer<Text, Text, NullWritable, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            JSONObject result = new JSONObject();
            double increate_line = 0.0;//正常还款增长额度
            double rate_des = 0.0;//逾期还款减额系数
            int loan_max_out_day = 0;  //最大逾期天数
            boolean first_flag = true;//首次贷款
            for (Text value : values) {
                JSONObject object = JSONObject.parseObject(value.toString());

                //1、历史贷款信息 INPUT_LOAN-> MAP_OUT
                String loan_repay_status = object.getString("loan_repay_status");
                String loan_check_status = object.getString("loan_check_status");
                if (loan_repay_status!= null) {
                    if ("s".equalsIgnoreCase(loan_check_status)) {
                        first_flag = false;
                    }
                    if ("s".equalsIgnoreCase(loan_repay_status)) {
                        increate_line += object.getDouble("repay_nomarl_increate");
                    } else if (("os".equalsIgnoreCase(loan_repay_status))) {
                        rate_des += object.getDouble("repay_late_increate_rate");
                    }
                }
                if (object.getInteger("loan_out_day") != null && object.getInteger("loan_out_day") > loan_max_out_day) {
                    loan_max_out_day = object.getInteger("loan_out_day");
                }

                //2、IndicatorJob005输出的指标
                if (object.get("user_base_amount") != null) {
                    result.put("user_base_amount", object.getDouble("user_base_amount"));
                }
                if (object.get("device_activation") != null) {
                    result.put("device_activation", object.get("device_activation"));
                }
                if (object.get("device_this_phone") != null) {
                    result.put("device_this_phone", object.get("device_this_phone"));
                }
                if (object.get("user_loan_overdue") != null) {
                    result.put("user_loan_overdue", object.get("user_loan_overdue"));
                }

                //3、IndicatorJob008输出的指标
                if (object.get("break_ratio") != null) {
                    result.put("break_ratio", object.get("break_ratio"));
                }

            }
            double base_fee_rate = getBaseFeeRate(first_flag,result.getDouble("break_ratio"),loan_max_out_day);
            result.put("base_fee_rate",base_fee_rate);

            if (Rules.joinRule(result, "user_base_amount")) {
                // 最终可借金额 = 正常增长额+逾期增长额+初始额度
                double finalAmount = Math.min(increate_line + rate_des * result.getDouble("user_base_amount") + result.getDouble("user_base_amount"), 5000);
                result.put("final_amount", ChubaoUtil.getIntAmount(finalAmount));
                result.put("loan_max_out_day", loan_max_out_day);
                result.put("user_id", key.toString());
                context.write(NullWritable.get(), new Text(result.toJSONString()));
            }
        }

    }

    /**
     *
     * 功能描述: <br>
     * 获取小额借款的还款系数
     *
     * @param loanDays 借款期限
     * @param repayRate 还款期限占比
     * @return
     * @see [相关类/方法](可选)
     * @since [产品/模块版本](可选)
     */
    private static double getRepayFactor(int loanDays, double repayRate) {
        double repayFactor = 0;
        if (loanDays <= 20) {
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
    private static double getIncreseNormal(JSONObject loan) {
        if ("s".equalsIgnoreCase(loan.getString("loan_repay_status"))) {
            int loanDays = loan.getInteger("loan_days");// 贷款期限
            long loanTime = loan.getInteger("loan_time");// 贷款时间
            long finishRepayTime = loan.getLong("loan_end_time");// 还款完成时间
            /**
             * 还款期限占比 = 实际借款时间/计划借款期限
             */
            double repayRate = 0.0;
            long realLoanDays = ChubaoDateUtil.getDateInterval(finishRepayTime, loanTime);
            if(realLoanDays < 0){
            	return 0;//对于这种情况，默认不进行调额
            }else if (realLoanDays == 0) {
                repayRate = 1;
            } else {
                repayRate = (double) realLoanDays/loanDays;// 实际借款时间/协议(计划)借款时间
            }

            double repayFactor = getRepayFactor(loanDays, repayRate);
            double incrementAmount = Math.min(loan.getDouble("loan_amount") * 0.5 * repayFactor, 500);
            return incrementAmount;
        }else{
            return 0;
        }

    }

    /**
     *
     * 功能描述: <br>
     * 逾期还款用户借款增量比例
     *
     * @param outDays
     * @return
     * @see [相关类/方法](可选)
     * @since [产品/模块版本](可选)
     */
    private static double getRate(Integer outDays) {
        double rate = 0;
        if (outDays >= 1 && outDays <= 3) {
            rate = -0.3;
        }
        if (outDays >= 4 && outDays <= 8) {
            rate = -0.5;
        }
        if (outDays >= 9 && outDays <= 15) {
            rate = -0.8;
        }
        if (outDays > 15) {
            rate = -1;
        }
        return rate;
    }

    /**
     * 计算用户基础费率
     *
     * @param first_flag
     * @param break_ratio
     * @param loan_max_out_day
     * @return
     */
    private static double getBaseFeeRate(boolean first_flag, Double break_ratio, int loan_max_out_day) {
        if (first_flag) {
            if (break_ratio != null && break_ratio < 0.5) {
                return ChubaoConstants.FIRST_A_BASE_FEE_RATE;
            } else {
                return ChubaoConstants.FIRST_B_BASE_FEE_RATE;
            }
        } else {
            if (loan_max_out_day > 0) {
                if (break_ratio != null && break_ratio < 0.5) {
                    return ChubaoConstants.OLD_A_HAVE_OVERDUE_BASE_FEE_RATE;
                } else {
                    return ChubaoConstants.OLD_B_HAVE_OVERDUE_BASE_FEE_RATE;
                }
            } else {
                return ChubaoConstants.OLD_NO_OVERDUE_BASE_FEE_RATE;
            }
        }
    }

    @Override
    public void setJobSpecialInfo(Job job, Configuration conf,
			RunParams params,
			Map<String, Path> tempPaths) throws Exception{


        job.setMapperClass(NewIndicatorJob006.RationMapper.class);
        job.setReducerClass(NewIndicatorJob006.RationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
		
        // 输入路径
        FileInputFormat.addInputPath(job, ChubaoJobConfig.getInputPath(ChubaoJobConfig.INPUT_LOAN));
        FileInputFormat.addInputPath(job, tempPaths.get(IndicatorJob005.class.getName()));
        FileInputFormat.addInputPath(job, tempPaths.get(CallLogJob001.class.getName()));
        // 输出路径
        FileOutputFormat.setOutputPath(job, tempPaths.get(NewIndicatorJob006.class.getName()) );
        // 删除原有的输出
        HadoopUtil.deleteOutputFile(tempPaths.get(NewIndicatorJob006.class.getName()));
        
    }
    
    
	@Override
	public final HashSet<String> getDependingJobNames() {
        return Sets.newHashSet(IndicatorJob005.class.getName(), CallLogJob001.class.getName());
    }

}
