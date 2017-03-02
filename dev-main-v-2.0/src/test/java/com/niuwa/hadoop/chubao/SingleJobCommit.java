package com.niuwa.hadoop.chubao;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.niuwa.hadoop.chubao.job.IndicatorJob001;
import com.niuwa.hadoop.chubao.job.IndicatorJob002;
import com.niuwa.hadoop.chubao.job.IndicatorJob003;
import com.niuwa.hadoop.chubao.job.IndicatorJob004;
import com.niuwa.hadoop.chubao.job.IndicatorJob005;
import com.niuwa.hadoop.chubao.job.IndicatorJob006;
import com.niuwa.hadoop.chubao.job.JugementJob;
import com.niuwa.hadoop.chubao.utils.ChubaoDateUtil;
import com.niuwa.hadoop.util.DateUtil;
import com.niuwa.hadoop.util.HadoopUtil;

/**
 * 触宝白名单任务启动类
 * 
 * @author Administrator
 *
 */
public class SingleJobCommit {
	private static final Logger log= LoggerFactory.getLogger(SingleJobCommit.class);
	
	public static void main(String[] args) throws Exception {
		HadoopUtil.isWinOrLiux();
		
		Configuration conf = new Configuration();
		
		if (args.length != 0) {
			ChubaoJobConfig.setRootPath(args[0]);
		}
		
		// date of latest data
		if(args.length>1){
			ChubaoDateUtil.setDataLastedTime(DateUtil.parse(args[1]));
		}else{
			ChubaoDateUtil.setDataLastedTime();
		}
		
		// running mode 
		if(args.length>2 && Boolean.parseBoolean(args[2])){
			ChubaoJobConfig.setDebugMode(true);
		}	

		Map<String, Path> tempPaths= new HashMap<String, Path>();
		tempPaths.put(IndicatorJob001.class.getName(), ChubaoJobConfig.getTempPath(IndicatorJob001.class.getName()));
		tempPaths.put(IndicatorJob002.class.getName(), ChubaoJobConfig.getTempPath(IndicatorJob002.class.getName()));
		tempPaths.put(IndicatorJob003.class.getName(), ChubaoJobConfig.getTempPath(IndicatorJob003.class.getName()));
		tempPaths.put(IndicatorJob004.class.getName(), ChubaoJobConfig.getTempPath(IndicatorJob004.class.getName()));
		tempPaths.put(IndicatorJob005.class.getName(), ChubaoJobConfig.getTempPath(IndicatorJob005.class.getName()));
		tempPaths.put(IndicatorJob006.class.getName(), ChubaoJobConfig.getTempPath(IndicatorJob006.class.getName()));

		/**
		 *  job1 从calllog，按照呼出联系人分组统计
		 *  
		 *  通话数量
		 *  是否为通讯录中存在的联系人
		 */
		Job job = Job.getInstance(conf, IndicatorJob001.class.getName());
		job.setJar("E:\\workspace-chubao-hadoop\\dev-main-v-1.x\\chubao-wl-jar-with-dependencies.jar");
		job.setJarByClass(IndicatorJob001.class);
		job.setMapperClass(IndicatorJob001.UserIdAndOtherPhoneMapper.class);
		job.setCombinerClass(IndicatorJob001.SumByUserIdAndOtherPhoneReducer.class);
		job.setReducerClass(IndicatorJob001.SumByUserIdAndOtherPhoneReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// 输入路径
		FileInputFormat.addInputPath(job, ChubaoJobConfig.getInputPath(ChubaoJobConfig.INPUT_CALL_LOG));
		// 输出路径
		FileOutputFormat.setOutputPath(job, tempPaths.get(IndicatorJob001.class.getName()));
		// 删除原有的输出
		HadoopUtil.deleteOutputFile(tempPaths.get(IndicatorJob001.class.getName()));


		System.exit(job.waitForCompletion(true) ? 0 : 1);  	 
		
		log.info("[isDebugMode]{}", ChubaoJobConfig.isDebugMode());
		log.info("[dataLatestTime]{}", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ChubaoDateUtil.dataLastedTime.getTime()));
		log.info("[job params]{}", JSONObject.toJSONString(args));
	}

}
