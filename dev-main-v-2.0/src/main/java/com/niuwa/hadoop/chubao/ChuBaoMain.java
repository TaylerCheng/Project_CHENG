package com.niuwa.hadoop.chubao;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.niuwa.hadoop.chubao.enums.RuleCounter;
import com.niuwa.hadoop.chubao.job.*;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
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
public class ChuBaoMain {

	private static final Logger log= LoggerFactory.getLogger(Main.class);

	public static void main(String[] args) throws Exception {
//		HadoopUtil.isWinOrLiux();

		long startTime = System.currentTimeMillis();
		List<ControlledJob> controlledJobs = runJobs(args);
		long endTime = System.currentTimeMillis();

		/**
		 * 打印白名单和每条规则的通过数
		 */
		if (ChubaoJobConfig.isDebugMode() && controlledJobs != null) {
			log.info("\n" + controlledJobs.toString());
			for (ControlledJob controlledJob : controlledJobs) {
				if (controlledJob.getJobName().equals(NewJugementJob.class.getName())) {
					Counters counters = controlledJob.getJob().getCounters();
					log.info("white list passed:[{}]", counters.findCounter(RuleCounter.RULE_PASS).getValue());
					log.info("rule-1 passed:[{}]", counters.findCounter(RuleCounter.RULE_1).getValue());
					log.info("rule-2 passed:[{}]", counters.findCounter(RuleCounter.RULE_2).getValue());
					log.info("rule-3 passed:[{}]", counters.findCounter(RuleCounter.RULE_3).getValue());
					log.info("rule-5 passed:[{}]", counters.findCounter(RuleCounter.RULE_5).getValue());
					log.info("rule-6 passed:[{}]", counters.findCounter(RuleCounter.RULE_6).getValue());
					log.info("rule-7 passed:[{}]", counters.findCounter(RuleCounter.RULE_7).getValue());
					log.info("rule-8 passed:[{}]", counters.findCounter(RuleCounter.RULE_8).getValue());
					log.info("rule-9 passed:[{}]", counters.findCounter(RuleCounter.RULE_9).getValue());
				}
			}
		}

		log.info("[共耗时]:{}(s)", (endTime - startTime) / 1000);
		log.info("[isDebugMode]{}", ChubaoJobConfig.isDebugMode());
		log.info("[dataLatestTime]{}",
				new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ChubaoDateUtil.dataLastedTime.getTime()));
		log.info("[job params]{}", JSONObject.toJSONString(args));

	}

	private static List<ControlledJob> runJobs(String[] args) throws Exception {
		//生成命令行解析参数
		Options options = ChubaoCommandLine.buildOptions();
		//解析输入的命令行参数
		RunParams params = new RunParams();
		boolean isSuc = ChubaoCommandLine.parse(args, options, params);
		if(!isSuc){
			return null;
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
				jobControl.stop();
				return jobControl.getSuccessfulJobList();
			}
		}
	}

	//此处需要根据每次实际运行的job进行修改，main函数中的整个调用流程不用修改
	//每个元素为对应job的类名
	private static List<String> getAllJobsToBeRun(RunParams params){
		List<String> allJobsTobeRun = null;

		if(params.isSmall()) {
			allJobsTobeRun = Lists.newArrayList(CallLogJob001.class.getName(), CallLogJob002.class.getName(),
					IndicatorJob002.class.getName(), IndicatorJob005.class.getName(), NewIndicatorJob006.class.getName(),
					IndicatorJob007.class.getName(), LargeIndicatorJob006.class.getName(), NewJugementJob.class.getName());
		}else{
			allJobsTobeRun = Lists.newArrayList(LargeIndicatorJob001.class.getName(), LargeIndicatorJob002.class.getName(),
					LargeIndicatorJob003.class.getName(), LargeIndicatorJob004.class.getName(), LargeIndicatorJob005.class.getName(),
					LargeIndicatorJob006.class.getName(),
					LargeJugementJob.class.getName());
		}

		return allJobsTobeRun;
	}


}
