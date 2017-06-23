package com.niuwa.hadoop.chubao.job;

import java.io.*;
import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import com.niuwa.hadoop.chubao.ChubaoJobConfig;
import com.niuwa.hadoop.chubao.NiuwaMapper;
import com.niuwa.hadoop.chubao.NiuwaReducer;
import com.niuwa.hadoop.chubao.RunParams;
import com.niuwa.hadoop.util.HadoopUtil;
import org.apache.log4j.Logger;

/**读取IndicatorJob0041的输出
 * 输出{"user_id": "xxxx", "total_calls_from_tel_library": 2, "total_diff_num_called_from_tel_library": 4}
 * @author Administrator
 * @deprecated 规则4下线 since 2017/03/15
 */
@Deprecated
public class IndicatorJob0042 extends BaseJob {
	
	private static Set<String> telLibrary = new HashSet<>();
	
	private final HashSet<String> dependingJobsName = Sets.newHashSet(IndicatorJob0041.class.getName());
	
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
	
	
	public static class SumCallsFromTelLibraryReducer extends NiuwaReducer<Text, Text, NullWritable, Text> {
		private static Logger log = Logger.getLogger(SumCallsFromTelLibraryReducer.class);

		private Text outValue = new Text();
		private JSONObject outObj = new JSONObject();
		
		public void setup(Context context){
	        super.setup(context);

			File file = new File(ChubaoJobConfig.CONFIG_APP_LIBARAY_FILE_NAME);
			BufferedReader reader=null;
			try{
				reader = new BufferedReader(new FileReader(file));
				String tel = null;
				while((tel=reader.readLine()) != null){
					telLibrary.add(tel);
				}
				log.info("Load the config file successfully,the file path is " + file.getAbsolutePath());
			}catch(Exception e){
				log.error("Load the config file failed,the file path is " + file.getAbsolutePath(), e);
			}finally{
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
	        
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			int totalCallsFromTelLibrary = 0;
			int totalDiffNumCalledFromTelLibrary = 0;
			Set<String> calledSet = new HashSet<String>();
			
			for(Text val: values){
				JSONObject json = JSONObject.parseObject(val.toString());
				if(telLibrary.contains(json.getString("other_phone"))){
					totalCallsFromTelLibrary += json.getIntValue("total_call_num");
					calledSet.add(json.getString("other_phone"));
				}
			}
			
			totalDiffNumCalledFromTelLibrary = calledSet.size();
			
			outObj.put("user_id", key.toString());
			outObj.put("total_calls_from_tel_library", totalCallsFromTelLibrary);
			outObj.put("total_diff_num_called_from_tel_library", totalDiffNumCalledFromTelLibrary);
			outValue.set(outObj.toJSONString());
			context.write(NullWritable.get(), outValue);
		}

	}
	
	@Override
	public void setJobSpecialInfo(Job job, Configuration conf,
			RunParams params,
			Map<String, Path> tempPaths) throws Exception{
		
		
		job.setMapperClass(IndicatorJob0042.UserIdsMapper.class);
		job.setReducerClass(IndicatorJob0042.SumCallsFromTelLibraryReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// 传入全局变量缓存文件
		job.addCacheFile(ChubaoJobConfig.getConfigPath(ChubaoJobConfig.CONFIG_TEL_LIBARAY_FILE_NAME).toUri());
		
		
		// 输入路径
		FileInputFormat.addInputPath(job, tempPaths.get(IndicatorJob0041.class.getName()));
		// 输出路径
		FileOutputFormat.setOutputPath(job, tempPaths.get(IndicatorJob0042.class.getName()));
		// 删除原有的输出
		HadoopUtil.deleteOutputFile(tempPaths.get(IndicatorJob0042.class.getName()));
		
	}
	

	@Override
	public final HashSet<String> getDependingJobNames() {
		// TODO Auto-generated method stub
		return dependingJobsName;
	}
}
