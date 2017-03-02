package com.niuwa.hadoop.jobs.sample;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.alibaba.fastjson.JSONObject;
import com.niuwa.hadoop.chubao.ChubaoJobConfig;
import com.niuwa.hadoop.chubao.job.IndicatorJob001;
import com.niuwa.hadoop.chubao.job.IndicatorJob002;
import com.niuwa.hadoop.chubao.job.IndicatorJob003;
import com.niuwa.hadoop.chubao.job.IndicatorJob004;
import com.niuwa.hadoop.chubao.job.IndicatorJob0041;
import com.niuwa.hadoop.chubao.job.IndicatorJob0042;
import com.niuwa.hadoop.chubao.job.IndicatorJob005;
import com.niuwa.hadoop.chubao.job.IndicatorJob006;
import com.niuwa.hadoop.chubao.utils.ChubaoDateUtil;
import com.niuwa.hadoop.util.DateUtil;
import com.niuwa.hadoop.util.HadoopUtil;

public class IndicatorJob0041Test {
	
	public static void main(String[] args) throws Exception {
		
		HadoopUtil.isWinOrLiux();
		boolean isRunAtRemote= false;
		String jarPath="";
		
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

		// is running at clusters
		if(args.length>3){
			isRunAtRemote= true;
			jarPath= args[3];
		}			
		
		// 全局参数放到HDFS上
		Path globalConfig= ChubaoJobConfig.getTempPath("globle-conf.txt");
		JSONObject gf= new JSONObject();
		gf.put("isDebug", ChubaoJobConfig.isDebugMode());
		gf.put("rootPath", ChubaoJobConfig.getRootPath());
		gf.put("dataLastedTime", ChubaoDateUtil.dataLastedTime.getTime().getTime());
		FileSystem fs= FileSystem.get(globalConfig.toUri(), conf);
		if(fs.exists(globalConfig)){
			fs.delete(globalConfig, true);
		}
		FSDataOutputStream out= fs.create(globalConfig);
		out.writeBytes(gf.toJSONString());		
		fs.close();
		
		
		// 将中间结果文件目录放到一个map中方便查找
		Map<String, Path> tempPaths= new HashMap<String, Path>();
		tempPaths.put(IndicatorJob001.class.getName(), ChubaoJobConfig.getTempPath(IndicatorJob001.class.getName()));
		tempPaths.put(IndicatorJob002.class.getName(), ChubaoJobConfig.getTempPath(IndicatorJob002.class.getName()));
		tempPaths.put(IndicatorJob003.class.getName(), ChubaoJobConfig.getTempPath(IndicatorJob003.class.getName()));
		tempPaths.put(IndicatorJob004.class.getName(), ChubaoJobConfig.getTempPath(IndicatorJob004.class.getName()));
		tempPaths.put(IndicatorJob005.class.getName(), ChubaoJobConfig.getTempPath(IndicatorJob005.class.getName()));
		tempPaths.put(IndicatorJob006.class.getName(), ChubaoJobConfig.getTempPath(IndicatorJob006.class.getName()));
		tempPaths.put(IndicatorJob0041.class.getName(), ChubaoJobConfig.getTempPath(IndicatorJob0041.class.getName()));
		tempPaths.put(IndicatorJob0042.class.getName(), ChubaoJobConfig.getTempPath(IndicatorJob0042.class.getName()));
		
		/*
		 * 计算来自不同号码的被叫次数
		 * 
		*/
		Job calCallsNumByDiffTelJob = Job.getInstance(conf, IndicatorJob0041.class.getName());
		calCallsNumByDiffTelJob.setJarByClass(IndicatorJob0041.class);
		calCallsNumByDiffTelJob.setMapperClass(IndicatorJob0041.UserIdsMapper.class);
		calCallsNumByDiffTelJob.setCombinerClass(IndicatorJob0041.IntCombiner.class);
		calCallsNumByDiffTelJob.setReducerClass(IndicatorJob0041.SumCallsReducer.class);
		calCallsNumByDiffTelJob.setOutputKeyClass(Text.class);
		calCallsNumByDiffTelJob.setOutputValueClass(IntWritable.class);
		
		// 远程调用需要
		if(isRunAtRemote){
			calCallsNumByDiffTelJob.setJar(jarPath);
		}
		// 传入全局变量缓存文件
		calCallsNumByDiffTelJob.addCacheFile(globalConfig.toUri());
		// 输入路径
		FileInputFormat.addInputPath(calCallsNumByDiffTelJob, ChubaoJobConfig.getInputPath(ChubaoJobConfig.INPUT_CALL_LOG));
		// 输出路径
		FileOutputFormat.setOutputPath(calCallsNumByDiffTelJob, tempPaths.get(IndicatorJob0041.class.getName()));
		// 删除原有的输出
		HadoopUtil.deleteOutputFile(tempPaths.get(IndicatorJob0041.class.getName()));
		
		
		System.exit(calCallsNumByDiffTelJob.waitForCompletion(true) ? 0 : 1);
	}
}
