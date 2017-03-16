package com.niuwa.hadoop.chubao;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.niuwa.hadoop.chubao.job.*;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.niuwa.hadoop.chubao.utils.ChubaoDateUtil;
import com.niuwa.hadoop.util.HadoopUtil;

/**
 * 触宝白名单任务启动类
 * 
 * @author Administrator
 *
 */
public class Main {

	public static int[] rule = new int[10];

	private static final Logger log= LoggerFactory.getLogger(Main.class);
	
	public static void main(String[] args) throws Exception {
//		HadoopUtil.isWinOrLiux();

		long startTime = System.currentTimeMillis();
		runJobs(args);
		long endTime = System.currentTimeMillis();

		log.info("[共耗时]:{}(s)", (endTime - startTime) / 1000);
		log.info("[isDebugMode]{}", ChubaoJobConfig.isDebugMode());
		log.info("[dataLatestTime]{}",
				new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ChubaoDateUtil.dataLastedTime.getTime()));
		log.info("[job params]{}", JSONObject.toJSONString(args));

		if (ChubaoJobConfig.isDebugMode()) {
			log.info("white list passed:[{}]", rule[0]);
			for (int i = 1; i < rule.length; i++) {
				log.info("rule-" + i + " passed:[{}]", rule[i]);
			}
		}

	}

	private static void runJobs(String[] args) throws Exception {
		//生成命令行解析参数
		Options options = ChubaoCommandLine.buildOptions();
		//解析输入的命令行参数
		RunParams params = new RunParams();
		boolean isSuc = ChubaoCommandLine.parse(args, options, params);
		if(!isSuc){
			return;
		}

		//初始化触宝全局配置
		ChubaoJobConfig.initChubaoConfig(params);

		//初始化job运行配置
		Configuration conf = new Configuration();
		ChubaoJobConfig.initConfiguration(params, conf);

		//获取所有需要运行的job
		List<String> allJobsTobeRun = getAllJobsToBeRun(params);

		//将中间结果文件目录放到一个map中方便查找
		Map<String, Path> tempPaths= ChubaoJobConfig.genPathsToStoreTempResults(allJobsTobeRun, params.isSmall());

		//初始化control job
		Map<BaseJob, ControlledJob> controlledJobMap = new HashMap<>();
		ChubaoJobConfig.initControlledJobMap(params, conf, allJobsTobeRun, tempPaths, controlledJobMap);


		// 创建JobControl
		String jobControlName = params.isSmall()?"Small":"Large"+" White List JobGroup";
		JobControl jobControl = new JobControl(jobControlName);
		for(BaseJob baseJob : controlledJobMap.keySet()){
			jobControl.addJob(controlledJobMap.get(baseJob));
		}

		// 启动任务
		Thread jobControlThread = new Thread(jobControl);
		jobControlThread.start();
		while (true) {
			if (jobControl.allFinished()) {
				List<ControlledJob> successfulJobList = jobControl.getSuccessfulJobList();
				for (ControlledJob controlledJob : successfulJobList) {
					Job job = controlledJob.getJob();
					long startTime = job.getStartTime();
					long finishTime = job.getFinishTime();
					System.out.println();
				}
//				System.out.println(jobControl.getSuccessfulJobList());
				jobControl.stop();
				break;
			}
		}
	}

	//此处需要根据每次实际运行的job进行修改，main函数中的整个调用流程不用修改
	//每个元素为对应job的类名
	private static List<String> getAllJobsToBeRun(RunParams params){
		List<String> allJobsTobeRun = null;
		
		if(params.isSmall()) {
			allJobsTobeRun = Lists.newArrayList(CallLogJob001.class.getName(), CallLogJob002.class.getName(),
					IndicatorJob002.class.getName(), IndicatorJob005.class.getName(), IndicatorJob006.class.getName(),
					IndicatorJob007.class.getName(), LargeIndicatorJob006.class.getName(), JugementJob.class.getName());
		}else{
			allJobsTobeRun = Lists.newArrayList(LargeIndicatorJob001.class.getName(), LargeIndicatorJob002.class.getName(),
					LargeIndicatorJob003.class.getName(), LargeIndicatorJob004.class.getName(), LargeIndicatorJob005.class.getName(),
					LargeIndicatorJob006.class.getName(),
					LargeJugementJob.class.getName());
		}
		
		return allJobsTobeRun;
	}
	

}
