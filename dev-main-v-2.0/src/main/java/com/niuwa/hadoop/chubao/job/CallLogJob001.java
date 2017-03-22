package com.niuwa.hadoop.chubao.job;

import com.alibaba.fastjson.JSONObject;
import com.niuwa.hadoop.chubao.ChubaoJobConfig;
import com.niuwa.hadoop.chubao.NiuwaMapper;
import com.niuwa.hadoop.chubao.NiuwaReducer;
import com.niuwa.hadoop.chubao.RunParams;
import com.niuwa.hadoop.chubao.enums.CallTypeEnum;
import com.niuwa.hadoop.chubao.enums.OtherPhoneSegmentEnum;
import com.niuwa.hadoop.chubao.rules.Rules;
import com.niuwa.hadoop.chubao.utils.ChubaoDateUtil;
import com.niuwa.hadoop.chubao.utils.ChubaoUtil;
import com.niuwa.hadoop.util.HadoopUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * call_log 计算出相关指标
 *
 * @author： Cheng Guang
 * @date： 2017/3/22.
 */
public class CallLogJob001 extends BaseJob {

    public static class CallLogMapper extends NiuwaMapper<LongWritable, Text, Text, Text> {
        private Text outKey = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject callLog = JSONObject.parseObject(value.toString());
            String user_id = callLog.getString("user_id");
            String other_phone = callLog.getString("other_phone");
            outKey.set(user_id + "\t" + other_phone);
            context.write(outKey, value);
        }

    }

    public static class CallLogReducer extends NiuwaReducer<Text, Text, NullWritable, Text> {
        private Text outValue = new Text();
        private JSONObject outObj = new JSONObject();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //原IndicatorJob001指标
            int call_sum = 0;//通话记录数
            int call_type_1_sum = 0;//呼叫类型为1的通话记录数
            boolean call_contact_true_flag = false;

            //原IndicatorJob003指标
            int last_3_call_sum = 0;//最近三个月通话记录数

            //原IndicatorJob008指标
            boolean not_in_contact_flag = false;    //不在通讯录标记
            boolean hang_up_flag = false;    //挂断标记

            //原IndicatorJob009指标
            int ttl_cnt = 0;    //通话次数
            int connected_call_sum = 0;    //接通次数
            int calling_sum = 0;    //拨出次数
            int called_sum = 0;    //拨入次数
            int connected_called_sum = 0;    //拨入接通次数
            Long total_call_time = 0L;    //总通话时长
            boolean contact_flag = false;    //在通讯录标志

            for (Text value : values) {
                JSONObject callLog = JSONObject.parseObject(value.toString());

                //原IndicatorJob001指标计算
                int call_type = callLog.getInteger("call_type");//通话类型
                boolean call_contact = callLog.getBoolean("call_contact");//通话号码是否在通讯录中
                long device_activation = callLog.getLong("device_activation");//设备激活时间
                if (Rules.callLogBaseRule(callLog)
                        && (ChubaoJobConfig.isDebugMode() || Rules.isMatchedRule_1(device_activation))) {
                    call_sum++;
                    call_type_1_sum += call_type;
                    call_contact_true_flag = call_contact_true_flag || call_contact;
                }

                //原IndicatorJob003指标计算
                if (Rules.isMatchedRule_2_1(callLog.getLong("call_date")) && (ChubaoJobConfig.isDebugMode() || Rules
                        .isMatchedRule_1(callLog.getLong("device_activation")))) {
                    last_3_call_sum++;
                }

                //原IndicatorJob008指标计算
                String other_phone = callLog.getString("other_phone");
                if (ChubaoUtil.telVilidate(other_phone)) {
                    if (!call_contact && ChubaoDateUtil.isBeforeNMonths(0, callLog.getLongValue("call_date") * 1000)) {
                        not_in_contact_flag = true;
                        if (callLog.getIntValue("call_duration") == 0) {
                            hang_up_flag = true;
                        }
                    }
                }

                //原IndicatorJob009指标计算
                if (StringUtils.isNotEmpty(other_phone)) {
                    int callDuration = callLog.getIntValue("call_duration");//通话时长
                    int callType = callLog.getIntValue("call_type");//通话类型	0:被叫 1:主叫
                    ttl_cnt++;
                    if (callDuration > 0) {
                        connected_call_sum++;
                        total_call_time += callDuration;
                    }
                    if (CallTypeEnum.CALLING.getId() == callType) {
                        calling_sum++;
                    } else if (CallTypeEnum.CALLED.getId() == callType) {
                        called_sum++;
                        if (callDuration > 0) {
                            connected_called_sum++;
                        }
                    }
                    contact_flag = contact_flag || call_contact;
                }

            }
            //原IndicatorJob001输出
            outObj.put("call_sum", call_sum);
            outObj.put("call_type_1_sum", call_type_1_sum);
            outObj.put("call_contact_true_flag", call_contact_true_flag);

            //原IndicatorJob003map输出
            outObj.put("last_3_call_sum", last_3_call_sum);

            //原IndicatorJob008map输出
            outObj.put("not_in_contact_flag", not_in_contact_flag);
            outObj.put("hang_up_flag", hang_up_flag);

            //原IndicatorJob009输出
            OtherPhoneSegmentEnum segementEnum = computeOtherPhoneSegement(ttl_cnt, connected_call_sum,
                    calling_sum, called_sum, connected_called_sum, total_call_time,
                    contact_flag);
            outObj.put("ttl_cnt", ttl_cnt);
            outObj.put("other_phone_segement", segementEnum.getSegment());

            String[] keys = key.toString().split("\t");
            outObj.put("user_id", keys[0]);
            if (keys.length == 2) {
                outObj.put("other_phone", keys[1]);
            }
            outValue.set(outObj.toJSONString());
            context.write(NullWritable.get(), outValue);
        }

        /**
         * 计算用户的联系号码的分段，分段逻辑如下：
         * 对于good和mid判断的前提是号码必须在通讯录中，并同时有拨入和拨出记录
         * good
         * a. 接通次数占比>=0.5 And 拨入接通次数占比>=0.6 And 通话次数<50
         * b. 接通次数占比>=0.5 And 拨入接通次数占比>=0.6 And 通话次数>=50 And 平均通话时长>=1.5min
         * c. 接通次数占比>=0.5 And 拨入接通次数占比<0.6 And 平均通话时长>=1min
         * mid:
         * a. 接通次数占比>=0.5 And 拨入接通次数占比>=0.6 And 通话次数>=50 And 平均通话时长<1.5min
         * b. 接通次数占比>=0.5 And 拨入接通次数占比<0.6 And 平均通话时长<1min
         * c. 接通次数占比<0.5
         * bad:除了上述两种号码外，其他都归到此类
         *
         * @param ttl_cnt              通话次数
         * @param connected_call_sum   接通次数
         * @param calling_sum          拨出
         * @param called_sum           拨入次数
         * @param connected_called_sum 拨入接通次数
         * @param total_call_time      总通话时长
         * @param contact_flag         是否在通讯录中
         * @return
         */
        private OtherPhoneSegmentEnum computeOtherPhoneSegement(int ttl_cnt, int connected_call_sum, int calling_sum,
                int called_sum,
                int connected_called_sum, Long total_call_time, boolean contact_flag) {
            if (calling_sum > 0 && called_sum > 0 && contact_flag) {
                double connect_rate = 1.0 * connected_call_sum / ttl_cnt;//接通次数占比=接通次数/通话次数
                double call_in_connect_rate = 1.0 * connected_called_sum / called_sum; //拨入接通次数占比=拨入接通次数/拨入次数
                double mean_call_duration = 0;
                if (connected_call_sum > 0) {
                    mean_call_duration = 1.0 * total_call_time / connected_call_sum;    //平均通话时长=总通话时长/接通次数
                }
                if (connect_rate >= 0.5) {
                    if (call_in_connect_rate >= 0.6) {
                        if (ttl_cnt < 50) {
                            return OtherPhoneSegmentEnum.GOOD;
                        } else {
                            if (mean_call_duration >= 90) {
                                return OtherPhoneSegmentEnum.GOOD;
                            } else {
                                return OtherPhoneSegmentEnum.MID;
                            }
                        }
                    } else {
                        if (mean_call_duration >= 60) {
                            return OtherPhoneSegmentEnum.GOOD;
                        } else {
                            return OtherPhoneSegmentEnum.MID;
                        }
                    }
                } else {
                    return OtherPhoneSegmentEnum.MID;
                }
            }
            return OtherPhoneSegmentEnum.BAD;
        }

    }

    @Override
    public void setJobSpecialInfo(Job job, Configuration conf, RunParams params, Map<String, Path> tempPaths)
            throws Exception {

        job.setMapperClass(CallLogJob001.CallLogMapper.class);
        job.setReducerClass(CallLogJob001.CallLogReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 输入路径
        FileInputFormat.addInputPath(job, ChubaoJobConfig.getInputPath(ChubaoJobConfig.INPUT_CALL_LOG));
        // 输出路径
        FileOutputFormat.setOutputPath(job, tempPaths.get(CallLogJob001.class.getName()));
        // 删除原有的输出
        HadoopUtil.deleteOutputFile(tempPaths.get(CallLogJob001.class.getName()));

    }

}