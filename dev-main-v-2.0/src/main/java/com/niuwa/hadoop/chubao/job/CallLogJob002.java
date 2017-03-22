package com.niuwa.hadoop.chubao.job;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import com.niuwa.hadoop.chubao.NiuwaMapper;
import com.niuwa.hadoop.chubao.NiuwaReducer;
import com.niuwa.hadoop.chubao.RunParams;
import com.niuwa.hadoop.chubao.enums.OtherPhoneSegmentEnum;
import com.niuwa.hadoop.util.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class CallLogJob002 extends BaseJob {

	public static class CallLogMapper extends NiuwaMapper<LongWritable, Text, Text, Text> {
		private Text outKey = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			JSONObject callLog = JSONObject.parseObject(value.toString());
			String user_id = callLog.getString("user_id");
			outKey.set(user_id);
			context.write(outKey, value);
		}

	}


	public static class CallLogReducer extends NiuwaReducer<Text, Text, NullWritable, Text> {
		private Text outValue = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			JSONObject outObj = new JSONObject();

			//原IndicatorJob003统计字段
			int total_call_num_3_month = 0;//客户最近三个月通话记录数

			//原IndicatorJob004统计字段(排除非手机号码通话记录，取最近6个月通话记录)
			int total_call_sum = 0;//总呼叫数
			int call_out_6_month_sum = 0;//总呼出号码数（排除总呼出数=1的号码）
			int call_out_true_6_month_sum = 0;//总呼出标记为true号码数（排除总呼出数=1的号码）
			int maxSum = 0;//最大呼叫数
			String max_contact_call_number = "";//最频繁呼出号码
			Boolean max_contact_call_status = false;//最频繁呼出号码状态
			List<Integer> top5Call = new ArrayList<Integer>();//最频繁呼出前5号码
			Comparator<Integer> intComparator = new Comparator<Integer>() {
				@Override
				public int compare(Integer o1, Integer o2) {
					return o1 - o2;
				}
			};

			//原IndicatorJob008计算字段
			double break_ratio = 0.0;//通讯录外手机号码挂断率
			int phone_num_not_in_contact_num = 0;//通讯录外通话总号码数
			int hang_up_calls_not_in_contact_num = 0;//通讯录外手机通话曾挂断总号码数

			//原IndicatorJob010计算字段
			int good_call_sum = 0;//好电话拨打次数
			int bad_call_sum = 0;//坏电话拨打次数
			int total_ttl_cnt = 0;//总拨打次数

			for (Text value : values) {
				JSONObject callLog = JSONObject.parseObject(value.toString());

                //1、原IndicatorJob003计算指标
				int last_3_call_sum = callLog.getIntValue("last_3_call_sum");
				total_call_num_3_month += last_3_call_sum;

				//2、原IndicatorJob004计算指标
				String other_phone = callLog.getString("other_phone");
				int call_sum = callLog.getIntValue("call_sum");//通话记录数
				int call_type_1_sum = callLog.getIntValue("call_type_1_sum");//呼叫类型为1的通话记录数
				boolean call_contact_true_flag = callLog.getBooleanValue("call_contact_true_flag");

				total_call_sum += call_sum;
				// 排除总呼出数=1的号码
				if (call_type_1_sum > 1) {
					call_out_6_month_sum++;
					if (call_contact_true_flag) {
						call_out_true_6_month_sum++;
					}
				}
				if (maxSum < call_sum) {
					maxSum = call_sum;
					max_contact_call_number = other_phone;
					max_contact_call_status = call_contact_true_flag;
				}
				if (maxSum == call_sum) {
					max_contact_call_status = max_contact_call_status || call_contact_true_flag;
				}
				top5Call.add(call_sum);
				Collections.sort(top5Call, intComparator);
				if (top5Call.size() > 5) {
					top5Call.remove(0);
				}

                //3、原IndicatorJob008计算指标
				boolean not_in_contact_flag = callLog.getBooleanValue("not_in_contact_flag");
				boolean hang_up_flag = callLog.getBooleanValue("hang_up_flag");
				if (not_in_contact_flag){
					phone_num_not_in_contact_num++;
					if (hang_up_flag){
						hang_up_calls_not_in_contact_num++;
					}
				}

				//4、原IndicatorJob010
				int ttl_cnt = callLog.getIntValue("ttl_cnt");
				String other_phone_segement = callLog.getString("other_phone_segement");
				total_ttl_cnt += ttl_cnt;
				if (OtherPhoneSegmentEnum.GOOD.getSegment().equals(other_phone_segement)) {
					good_call_sum += ttl_cnt;
				} else if (OtherPhoneSegmentEnum.BAD.getSegment().equals(other_phone_segement)) {
					bad_call_sum += ttl_cnt;
				}

			}
            //原IndicatorJob003输出
			outObj.put("call_num_3_month", total_call_num_3_month);

			//原IndicatorJob004输出
			int top5sum_call = 0;//最频繁呼出前5号码总呼出数
			for (Integer mapkey : top5Call) {
				top5sum_call += mapkey;
			}
			double call_top5_perct_type ;//最频繁呼出前5号码总呼出数占总呼出数占比
			double call_true_rate_type ;//呼出电话中标记为true的号码数占呼出总号码数的占比(若某号码曾经被标记为true, 则该号码记为true, 排除总呼出数=1的号码)
			if (total_call_sum == 0) {
				call_top5_perct_type = 0.0;
			} else {
				call_top5_perct_type = 1.0 * top5sum_call / total_call_sum;
			}
			if (call_out_6_month_sum == 0) {
				call_true_rate_type = 0.0;
			} else {
				call_true_rate_type = 1.0 * call_out_true_6_month_sum / call_out_6_month_sum;
			}
			if (call_true_rate_type >= 0.3) {
				outObj.put("call_true_rate_type", call_true_rate_type);
				outObj.put("call_out_6_month_sum", call_out_6_month_sum);
				outObj.put("call_out_true_6_month_sum", call_out_true_6_month_sum);
				outObj.put("max_contact_call", max_contact_call_status);
				outObj.put("max_contact_call_number", max_contact_call_number);
				outObj.put("call_top5_perct_type", call_top5_perct_type);
				outObj.put("total_call", total_call_sum);
				outObj.put("top5sum_call", top5sum_call);
			}

			//原IndicatorJob008输出
			if(phone_num_not_in_contact_num > 0){
				break_ratio = 1.0 * hang_up_calls_not_in_contact_num / phone_num_not_in_contact_num;
			}
			outObj.put("break_ratio", break_ratio);

			//原IndicatorJob010输出
			double good_cnt_rate = 0.0;//好电话拨打次数占比（好电话拨打次数/总拨打次数）
			double bad_cnt_rate = 0.0;//坏电话拨打次数占比（坏电话拨打次数/总拨打次数）
			if (total_ttl_cnt > 0) {
				good_cnt_rate = 1.0 * good_call_sum / total_ttl_cnt;
				bad_cnt_rate = 1.0 * bad_call_sum / total_ttl_cnt;
			}
			outObj.put("good_cnt_rate",good_cnt_rate);
			outObj.put("bad_cnt_rate",bad_cnt_rate);

			//最终输出
			outObj.put("user_id", key.toString());
			outValue.set(outObj.toJSONString());
			context.write(NullWritable.get(), outValue);
		}
	}

	@Override
	public void setJobSpecialInfo(Job job, Configuration conf,
			RunParams params,
			Map<String, Path> tempPaths) throws Exception {

		job.setMapperClass(CallLogJob002.CallLogMapper.class);
		job.setReducerClass(CallLogJob002.CallLogReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// 输入路径
		FileInputFormat.addInputPath(job, tempPaths.get(CallLogJob001.class.getName()) );
		// 输出路径
		FileOutputFormat.setOutputPath(job, tempPaths.get(CallLogJob002.class.getName()) );
		// 删除原有的输出
		HadoopUtil.deleteOutputFile(tempPaths.get(CallLogJob002.class.getName()));
		
	}
	
	@Override
	public final HashSet<String> getDependingJobNames() {
		return Sets.newHashSet(CallLogJob001.class.getName());
	}
}
