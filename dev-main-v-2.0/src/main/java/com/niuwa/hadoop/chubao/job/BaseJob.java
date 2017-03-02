package com.niuwa.hadoop.chubao.job;

import java.util.HashSet;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

import com.niuwa.hadoop.chubao.RunParams;

/*
 * 作为所有job的父类，子类必须重写setJobSpecialInfo
 * 如果子类存在依赖的job，则必须重写getDependingJobNames
 */
public abstract class BaseJob {

	/**根据依赖job名字添加依赖的job
	 * @param job
	 * @param possibleDependingJob
	 */
	public void addDependingJobs(ControlledJob job, 
			Map<BaseJob,ControlledJob> possibleDependingJob){
		HashSet<String> set = getDependingJobNames();
		if(CollectionUtils.isEmpty(set)){
			return;
		}
		
		for(ControlledJob cJob: possibleDependingJob.values()){
			if(set.contains(cJob.getJobName())){
				job.addDependingJob(cJob);
			}
		}
	}

	public ControlledJob getControlledJob(Configuration conf,
			RunParams params,
			Map<String, Path> tempPaths) throws Exception{
		
		Job job = Job.getInstance(conf, this.getClass().getName());
		
		//设置所有job通用的属性
		setJobCommonInfo(job, params);
		//设置job特有的属性
		setJobSpecialInfo(job, conf, params, tempPaths);
		
		return new ControlledJob(job.getConfiguration());
	}
	
	
	/**如果子类存在依赖job，则必须重写该类
	 * 默认子类是没有依赖job的
	 * @return
	 */
	public HashSet<String> getDependingJobNames(){
		return null;
	}
	
	
	
	public abstract void setJobSpecialInfo(Job job, Configuration conf,
			RunParams params,
			Map<String, Path> tempPaths) throws Exception;
	
	
	/**设置job共有的属性
	 * @param job
	 * @param params
	 */
	private void setJobCommonInfo(Job job, RunParams params){
		job.setJarByClass(this.getClass());
		job.setNumReduceTasks(params.getDefaultReducerTaskNum());
		// 远程调用需要
		if(params.isRunAtRemote()){
			job.setJar(params.getJarPath());
		}
	}
}
