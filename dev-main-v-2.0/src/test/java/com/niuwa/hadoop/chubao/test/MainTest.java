package com.niuwa.hadoop.chubao.test;

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
import com.niuwa.hadoop.chubao.ChubaoJobConfig;
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
public class MainTest {
	private static final Logger log= LoggerFactory.getLogger(MainTest.class);
	
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

		
		/**
		 *  job2  根据job1的结果按照用户统计
		 *  
		 *  频繁呼出号码占比
		 *  最频繁呼出号码是否在通讯录
		 *  最频繁的前五号码通话占比
		 */
		Job job2 = Job.getInstance(conf, "call_log_filter_group_next");
		job2.setJarByClass(IndicatorJob004.class);
		job2.setMapperClass(IndicatorJob004.UserIdMapper.class);
		job2.setReducerClass(IndicatorJob004.Sum1Reducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		// 输入路径
		FileInputFormat.addInputPath(job2,  tempPaths.get(IndicatorJob001.class.getName()) );
		// 输出路径
		FileOutputFormat.setOutputPath(job2, tempPaths.get(IndicatorJob004.class.getName()) );
		// 删除原有的输出
		HadoopUtil.deleteOutputFile( tempPaths.get(IndicatorJob004.class.getName()));
		
		
		/**
		 *  job3  call_log按用户统计
		 *  
		 *  最新三月通话记录
		 */
/*		Job job3 = Job.getInstance(conf, "call_log_3_sum");
		job3.setJarByClass(IndicatorJob003.class);
		job3.setMapperClass(IndicatorJob003.CallLogMapper.class);
		job3.setCombinerClass(IndicatorJob003.IntCombiner.class);
		job3.setReducerClass(IndicatorJob003.IntSumReducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(IntWritable.class);
		// 输入路径
		FileInputFormat.addInputPath(job3, ChubaoJobConfig.getInputPath(ChubaoJobConfig.INPUT_CALL_LOG));
		// 输出路径
		FileOutputFormat.setOutputPath(job3, tempPaths.get(IndicatorJob003.class.getName()) );
		// 删除原有的输出
		HadoopUtil.deleteOutputFile(tempPaths.get(IndicatorJob003.class.getName()));*/
		
		/**
		 *  job4 contact按用户统计
		 */
		Job job4 = Job.getInstance(conf, "concat_sum");
		job4.setJarByClass(IndicatorJob002.class);
		job4.setMapperClass(IndicatorJob002.UserIdMapper.class);
		job4.setCombinerClass(IndicatorJob002.CombinerSumReducer.class);
		job4.setReducerClass(IndicatorJob002.IntSumReducer.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(IntWritable.class);
		// 输入路径
		FileInputFormat.addInputPath(job4, ChubaoJobConfig.getInputPath(ChubaoJobConfig.INPUT_CONTACT));
		// 输出路径
		FileOutputFormat.setOutputPath(job4,  tempPaths.get(IndicatorJob002.class.getName()) );
		// 删除原有的输出
		HadoopUtil.deleteOutputFile( tempPaths.get(IndicatorJob002.class.getName()));

		/**
		 * job5定价指标
		 */
		Job job5 = Job.getInstance(conf, "user_rank_price");
        job5.setJarByClass(IndicatorJob005.class);
        job5.setMapperClass(IndicatorJob005.PriceRuleMapper.class);
        job5.setOutputKeyClass(NullWritable.class);
        job5.setOutputValueClass(Text.class);
        // 缓存配置文件
        job5.addCacheFile(ChubaoJobConfig.getConfigPath("static-mobile-tier.txt").toUri());
        // 输入路径
        FileInputFormat.addInputPath(job5, ChubaoJobConfig.getInputPath(ChubaoJobConfig.INPUT_USER_INFO));
		// 输出路径
        FileOutputFormat.setOutputPath(job5, tempPaths.get(IndicatorJob005.class.getName()) );
        // 删除原有的输出
        HadoopUtil.deleteOutputFile( tempPaths.get(IndicatorJob005.class.getName()));
        
        /**
         * job6定额指标
         */
        Job job6 = Job.getInstance(conf, "small_ration_param");
        job6.setJarByClass(IndicatorJob006.class);
        job6.setMapperClass(IndicatorJob006.RationMapper.class);
        job6.setReducerClass(IndicatorJob006.RationReducer.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(Text.class);
        // 输入路径
        //FileInputFormat.addInputPath(job6, ChubaoJobConfig.getInputPath(ChubaoJobConfig.INPUT_LOAN));
        FileInputFormat.addInputPath(job6, tempPaths.get(IndicatorJob005.class.getName())); // 依赖job：IndicatorJob005
        // 输出路径
        FileOutputFormat.setOutputPath(job6, tempPaths.get(IndicatorJob006.class.getName()) );
        // 删除原有的输出
        HadoopUtil.deleteOutputFile(tempPaths.get(IndicatorJob006.class.getName()));
        
		/**
		 *  job7  合并技术指标
		 */
		Job job7 = Job.getInstance(conf, "join_profile");
		job7.setJarByClass(JugementJob.class);
		job7.setMapperClass(JugementJob.MapSevralTemp.class);
		job7.setReducerClass(JugementJob.ReduceObj.class);
		job7.setOutputKeyClass(Text.class);
		job7.setOutputValueClass(Text.class);
		// 读取静态缓存的费率配置文件
		job7.addCacheFile(ChubaoJobConfig.getConfigPath("rate-config.txt").toUri());
        // 输入路径
		FileInputFormat.addInputPath(job7, tempPaths.get(IndicatorJob002.class.getName()));
		FileInputFormat.addInputPath(job7, tempPaths.get(IndicatorJob003.class.getName()));
		FileInputFormat.addInputPath(job7, tempPaths.get(IndicatorJob004.class.getName()));
		FileInputFormat.addInputPath(job7, tempPaths.get(IndicatorJob006.class.getName()));
		// 输出路径
		FileOutputFormat.setOutputPath(job7, ChubaoJobConfig.getOutputPath("small"));
		// 删除原有的输出
		HadoopUtil.deleteOutputFile(ChubaoJobConfig.getOutputPath("small"));
        
		// 创建ControlledJob
		ControlledJob controlledJob1 = new ControlledJob(job.getConfiguration());
		ControlledJob controlledJob2 = new ControlledJob(job2.getConfiguration());
		//ControlledJob controlledJob3 = new ControlledJob(job3.getConfiguration());
		ControlledJob controlledJob4 = new ControlledJob(job4.getConfiguration());
		ControlledJob controlledJob5 = new ControlledJob(job5.getConfiguration());
		ControlledJob controlledjob6 = new ControlledJob(job6.getConfiguration());
		ControlledJob controlledjob7 = new ControlledJob(job7.getConfiguration());
		
		// 添加依赖
		controlledJob2.addDependingJob(controlledJob1);
		controlledjob6.addDependingJob(controlledJob5);
		controlledjob7.addDependingJob(controlledJob2);
		//controlledjob7.addDependingJob(controlledJob3);
		controlledjob7.addDependingJob(controlledJob4);
		controlledjob7.addDependingJob(controlledjob6);
		
		// 创建JobControl
		JobControl jobControl = new JobControl("JobGroup");
		jobControl.addJob(controlledJob1);
		jobControl.addJob(controlledJob2);
		//jobControl.addJob(controlledJob3);
		jobControl.addJob(controlledJob4);
		jobControl.addJob(controlledJob5);
		jobControl.addJob(controlledjob7);
		jobControl.addJob(controlledjob6);
		
		// 启动任务
		Thread jobControlThread = new Thread(jobControl);
		jobControlThread.start();
		while (true) {
			if (jobControl.allFinished()) {
				System.out.println(jobControl.getSuccessfulJobList());
				jobControl.stop();
				break;
			}
		}
		
		log.info("[isDebugMode]{}", ChubaoJobConfig.isDebugMode());
		log.info("[dataLatestTime]{}", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ChubaoDateUtil.dataLastedTime.getTime()));
		log.info("[job params]{}", JSONObject.toJSONString(args));
	}

}
