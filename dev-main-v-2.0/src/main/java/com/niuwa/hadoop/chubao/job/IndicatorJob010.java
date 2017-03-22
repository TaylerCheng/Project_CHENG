package com.niuwa.hadoop.chubao.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import com.niuwa.hadoop.chubao.NiuwaMapper;
import com.niuwa.hadoop.chubao.NiuwaReducer;
import com.niuwa.hadoop.chubao.RunParams;
import com.niuwa.hadoop.chubao.enums.OtherPhoneSegmentEnum;
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
 * 计算好电话拨打次数占比和坏电话拨打次数占比
 *
 * @rule 规则8
 * @input call_log[通话记录]
 * @outputkey user_id	 用户id
 * @indicator good_cnt_rate 好电话拨打次数占比
 * @indicator bad_cnt_rate 坏电话拨打次数占比
 *
 * @author ChengGuang
 * @date: 2017/02/23
 */
public class IndicatorJob010 extends BaseJob{

	public static class UserIdsMapper extends NiuwaMapper<Object, Text, Text, Text> {
		private Text outKey = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			JSONObject callLogJson = JSON.parseObject(value.toString());

			String userId = callLogJson.getString("user_id");
			outKey.set(userId);
			context.write(outKey, value);
		}

	}

	public static class CallReducer extends NiuwaReducer<Text, Text, NullWritable, Text> {

		private Text outValue = new Text();
		private JSONObject outObj = new JSONObject();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int good_call_sum = 0;//好电话拨打次数
			int bad_call_sum = 0;//坏电话拨打次数
			int total_call_sum = 0;//总拨打次数

			for (Text val : values) {
				JSONObject json = JSONObject.parseObject(val.toString());
				int ttl_cnt = json.getIntValue("ttl_cnt");
				String other_phone_segement = json.getString("other_phone_segement");

				total_call_sum += ttl_cnt;
				if (OtherPhoneSegmentEnum.GOOD.getSegment().equals(other_phone_segement)) {
					good_call_sum += ttl_cnt;
				} else if (OtherPhoneSegmentEnum.BAD.getSegment().equals(other_phone_segement)) {
					bad_call_sum += ttl_cnt;
				}
			}
			double good_cnt_rate = 0.0;//好电话拨打次数占比（好电话拨打次数/总拨打次数）
			double bad_cnt_rate = 0.0;//坏电话拨打次数占比（坏电话拨打次数/总拨打次数）
			if (total_call_sum > 0) {
				good_cnt_rate = 1.0 * good_call_sum / total_call_sum;
				bad_cnt_rate = 1.0 * bad_call_sum / total_call_sum;
			}
			outObj.put("user_id", key.toString());
			outObj.put("good_cnt_rate",good_cnt_rate);
			outObj.put("bad_cnt_rate",bad_cnt_rate);
			outValue.set(outObj.toJSONString());
			context.write(NullWritable.get(),outValue);
		}
	}

	@Override
	public void setJobSpecialInfo(Job job, Configuration conf,
			RunParams params,
			Map<String, Path> tempPaths) throws Exception {

		job.setMapperClass(IndicatorJob010.UserIdsMapper.class);
		job.setReducerClass(IndicatorJob010.CallReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 输入路径
		FileInputFormat.addInputPath(job, tempPaths.get(IndicatorJob009.class.getName()));
		// 输出路径
		FileOutputFormat.setOutputPath(job, tempPaths.get(IndicatorJob010.class.getName()));
		// 删除原有的输出
		HadoopUtil.deleteOutputFile(tempPaths.get(IndicatorJob010.class.getName()));

	}

	@Override
	public final HashSet<String> getDependingJobNames() {
		return Sets.newHashSet(IndicatorJob009.class.getName());
	}
}
