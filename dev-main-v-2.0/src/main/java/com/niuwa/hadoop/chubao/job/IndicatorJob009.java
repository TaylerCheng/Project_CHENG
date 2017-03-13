package com.niuwa.hadoop.chubao.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.niuwa.hadoop.chubao.ChubaoJobConfig;
import com.niuwa.hadoop.chubao.NiuwaMapper;
import com.niuwa.hadoop.chubao.NiuwaReducer;
import com.niuwa.hadoop.chubao.RunParams;
import com.niuwa.hadoop.chubao.enums.CallTypeEnum;
import com.niuwa.hadoop.chubao.enums.OtherPhoneSegmentEnum;
import com.niuwa.hadoop.chubao.utils.ChubaoUtil;
import com.niuwa.hadoop.util.HadoopUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * 计算用户与每个号码的呼叫次数，呼叫类型以及号码分段
 *
 * @rule 规则8
 * @input call_log[通话记录]
 * @outputkey user_id+other_phone	用户拨打的电话
 * @indicator calling_sum	 拨打该电话次数
 * @indicator other_phone_segement	 该电话号码分段
 *
 * @author ChengGuang
 * @date: 2017/02/23
 */
public class IndicatorJob009 extends BaseJob{

	public static class UserIdsMapper extends NiuwaMapper<Object, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outValue = new Text();
		private JSONObject outObj = new JSONObject();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			JSONObject callLogJson = JSON.parseObject(value.toString());

			String userId = callLogJson.getString("user_id");
			String otherPhone = callLogJson.getString("other_phone");
			if (StringUtils.isNotEmpty(otherPhone)) {
				outKey.set(userId + "\t" + otherPhone);
				outObj.put("call_type", callLogJson.getIntValue("call_type"));
				outObj.put("call_duration", callLogJson.getIntValue("call_duration"));
				outObj.put("call_contact", callLogJson.getBoolean("call_contact"));
				outValue.set(outObj.toJSONString());
				context.write(outKey, outValue);
			}
		}

	}


	public static class CallReducer extends NiuwaReducer<Text, Text, NullWritable, Text> {

		private Text outValue = new Text();
		private JSONObject outObj = new JSONObject();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int ttl_cnt = 0;	//通话次数
			int connected_call_sum = 0;	//接通次数
			int calling_sum = 0;	//拨出次数
			int called_sum = 0;    //拨入次数
			int connected_called_sum = 0;	//拨入接通次数
			Long total_call_time = 0L;	//总通话时长
			boolean contact_flag = false;    //通讯录标志

			for(Text value : values) {
				JSONObject callLogJson = JSONObject.parseObject(value.toString());
				int callDuration = callLogJson.getIntValue("call_duration");//通话时长
				int callType = callLogJson.getIntValue("call_type");//通话类型	0:被叫 1:主叫
				boolean callContact = callLogJson.getBoolean("call_contact");//通话号码是否在通讯录中

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
				contact_flag = contact_flag || callContact;
			}
			String[] keys = key.toString().split("\t");
			if ("1004b0a00c79a7b2bb2968f2b08b54e9c04b85b1".equals(keys[0])) {
				System.out.println();
				if ("1372457****".equals(keys[1])){
					System.out.println();
				}
			}
			OtherPhoneSegmentEnum segementEnum = computeOtherPhoneSegement(ttl_cnt, connected_call_sum,
					calling_sum, called_sum, connected_called_sum, total_call_time,
					contact_flag);

			outObj.put("user_id", keys[0]);
			outObj.put("other_phone", keys[1]);
			outObj.put("ttl_cnt",ttl_cnt);
			outObj.put("other_phone_segement",segementEnum.getSegment());
			outValue.set(outObj.toJSONString());
			context.write(NullWritable.get(), outValue);
		}

		/**
		 * 计算用户的联系号码的分段，分段逻辑如下：
			 对于good和mid判断的前提是号码必须在通讯录中，并同时有拨入和拨出记录
			 good
			 a. 接通次数占比>=0.5 And 拨入接通次数占比>=0.6 And 通话次数<50
			 b. 接通次数占比>=0.5 And 拨入接通次数占比>=0.6 And 通话次数>=50 And 平均通话时长>=1.5min
			 c. 接通次数占比>=0.5 And 拨入接通次数占比<0.6 And 平均通话时长>=1min
			 mid:
			 a. 接通次数占比>=0.5 And 拨入接通次数占比>=0.6 And 通话次数>=50 And 平均通话时长<1.5min
			 b. 接通次数占比>=0.5 And 拨入接通次数占比<0.6 And 平均通话时长<1min
			 c. 接通次数占比<0.5
			 bad:除了上述两种号码外，其他都归到此类
		 * @param ttl_cnt 通话次数
		 * @param connected_call_sum 接通次数
		 * @param calling_sum 拨出
		 * @param called_sum 拨入次数
		 * @param connected_called_sum 拨入接通次数
		 * @param total_call_time 总通话时长
		 * @param contact_flag 是否在通讯录中
		 * @return
		 */
		private OtherPhoneSegmentEnum computeOtherPhoneSegement(int ttl_cnt, int connected_call_sum, int calling_sum, int called_sum,
				int connected_called_sum, Long total_call_time, boolean contact_flag) {
			if (calling_sum > 0 && called_sum > 0 && contact_flag) {
				double connect_rate = 1.0 * connected_call_sum / ttl_cnt;//接通次数占比=接通次数/通话次数
				double call_in_connect_rate = 1.0 * connected_called_sum / called_sum; //拨入接通次数占比=拨入接通次数/拨入次数
				int mean_call_duration = 0;
				if (connected_call_sum>0) {
					mean_call_duration = (int) (total_call_time / connected_call_sum);    //平均通话时长=总通话时长/接通次数
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
						if (mean_call_duration > 60) {
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
	public void setJobSpecialInfo(Job job, Configuration conf,
			RunParams params,
			Map<String, Path> tempPaths) throws Exception{

        job.setMapperClass(IndicatorJob009.UserIdsMapper.class);
        job.setReducerClass(IndicatorJob009.CallReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
		
        // 输入路径
        FileInputFormat.addInputPath(job, ChubaoJobConfig.getInputPath(ChubaoJobConfig.INPUT_CALL_LOG));
        // 输出路径
        FileOutputFormat.setOutputPath(job, tempPaths.get(IndicatorJob009.class.getName()) );
        // 删除原有的输出
        HadoopUtil.deleteOutputFile(tempPaths.get(IndicatorJob009.class.getName()));
        
	}

}
