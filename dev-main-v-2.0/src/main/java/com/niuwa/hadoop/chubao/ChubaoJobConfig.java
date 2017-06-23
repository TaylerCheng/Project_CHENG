package com.niuwa.hadoop.chubao;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

import com.niuwa.hadoop.chubao.job.BaseJob;
import com.niuwa.hadoop.chubao.utils.ChubaoDateUtil;
import com.niuwa.hadoop.util.DateUtil;


/**
 * 触宝白名单任务静态常量
 * 
 * @author wangjiazhi
 *
 */
public class ChubaoJobConfig {
	// 默认的hdfs运行路径
	private static String rootPath= "hdfs://ns1/user/root";	
	/**
	 * 作业是否按照debug模式运行
	 * 
	 * debug模式下，统计类作业执行优化过滤条件
	 * 
	 */
	private static boolean isDebugMode= false;
	
	// 输入文件相对路径
	public static final String relative_path_input= "/chubao/input";
	// 输入文件相对路径
	public static final String relative_path_config= "/chubao/config";
	// 中间过程结果相对路径
	public static final String relative_path_temp= "/chubao/temp/"+DateUtil.format(new Date());
	// 输出结果相对路径
	public static final String relative_path_output= "/chubao/output/"+ DateUtil.format(new Date());

	public static final String INPUT_CALL_LOG="call_log";
	public static final String INPUT_CONTACT="contact";
	public static final String INPUT_USER_INFO="user_info";
	public static final String INPUT_LOAN="loan";
	public static final String INPUT_APP="app";
	
	
	public static final String CONFIG_TEL_LIBARAY_FILE_NAME = "tel-library.txt";
	public static final String CONFIG_APP_LIBARAY_FILE_NAME = "app-library.txt";
	public static final String CONFIG_STATIC_MOBILE_TIER_FILE_NAME = "static-mobile-tier.txt";
	public static final String CONFIG_RATE_FILE_NAME = "rate-config.txt";

	/**
	 * 获取配置路径
	 * 
	 * @param name
	 * @return
	 */
	public static Path getConfigPath(String name){
		return getFilePath(rootPath+relative_path_config, name);
	}	
	
	/**
	 * 获取输出路径
	 * 
	 * @param name
	 * @return
	 */
	public static Path getOutputPath(String name){
		return getFilePath(rootPath+relative_path_output, name);
	}	
	
	/**
	 * 获取输入文件路径
	 * 
	 * @param name
	 * @return
	 */
	public static Path getTempPath(String name){
		return getFilePath(rootPath+relative_path_temp, name);
	}	
	
	/**
	 * 获取输入文件路径
	 * 
	 * @param name
	 * @return
	 */
	public static Path getInputPath(String name){
		return getFilePath(rootPath+relative_path_input, name);
	}
	
	/**
	 * 
	 * @param parent
	 * @param name
	 * @return
	 */
	public static Path getFilePath(String parent, String name){
		return new Path(parent, name);
	}	
	
	public static String getRootPath() {
		return rootPath;
	}
	public static void setRootPath(String rootPath) {
		ChubaoJobConfig.rootPath = rootPath;
	}
	
	public static boolean isDebugMode() {
		return isDebugMode;
	}
	public static void setDebugMode(boolean isDebugMode) {
		ChubaoJobConfig.isDebugMode = isDebugMode;
	}
	
	/**生成临时结果存放目录，生成规则为small.(或者large.)+对应job的类名
	 * 该生成规则不建议修改，后续job中会据此选择数据输入或者输出目录
	 * @param allJobsTobeRun
	 * @param isSmall
	 * @return
	 */
	public static Map<String, Path> genPathsToStoreTempResults(List<String> allJobsTobeRun, boolean isSmall){
		Map<String, Path> tempPaths= new HashMap<String, Path>();
		String prefix = "small.";
		if(!isSmall){
			prefix = "large.";
		}
		for(String str: allJobsTobeRun){
			tempPaths.put(str, ChubaoJobConfig.getTempPath(prefix+str));
		}
		return tempPaths;
	}
	
	/**初始化一些全局的配置
	 * @param params
	 */
	public static void initChubaoConfig(RunParams params){
		ChubaoJobConfig.setRootPath(params.getRootPath());//后续设置删除或者临时存放文件位置均依赖此设置
		ChubaoJobConfig.setDebugMode(params.isDebug());
		if(params.getTime() == null){
			ChubaoDateUtil.setDataLastedTime();//默认当前月第一天
		}else{
			try {
				ChubaoDateUtil.setDataLastedTime(DateUtil.parse(params.getTime()));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**初始化job配置信息，job在被分发到不同节点执行时，通过setup方法读取这些配置信息
	 * @param params
	 * @param conf
	 */
	public static void initConfiguration(RunParams params, Configuration conf){
		conf.set("isDebug", String.valueOf(params.isDebug()));
		conf.set("rootPath", String.valueOf(params.getRootPath()));
		conf.set("dataLastedTime", String.valueOf(ChubaoDateUtil.dataLastedTime.getTime().getTime()));
	}
	
	/**初始化，完成各个controlJob的配置
	 * @param params
	 * @param conf
	 * @param allJobsTobeRun
	 * @param tempPaths
	 * @param controlledJobMap
	 * @throws Exception
	 */
	public static void initControlledJobMap(RunParams params,
			Configuration conf, List<String> allJobsTobeRun,
			Map<String, Path> tempPaths, Map<BaseJob, ControlledJob> controlledJobMap) throws Exception {
		//根据jobName，初始化BaseJob信息
		for(String clzName: allJobsTobeRun){
			BaseJob key = (BaseJob) Class.forName(clzName).newInstance();
			controlledJobMap.put(key, null);
		}
		//初始化controlledJob信息
		for(BaseJob base: controlledJobMap.keySet()){
			controlledJobMap.put(base, base.getControlledJob(conf, params, tempPaths));
		}
		//添加依赖job
		for(BaseJob base: controlledJobMap.keySet()){
			base.addDependingJobs(controlledJobMap.get(base), controlledJobMap);
		}
	}
	
}
