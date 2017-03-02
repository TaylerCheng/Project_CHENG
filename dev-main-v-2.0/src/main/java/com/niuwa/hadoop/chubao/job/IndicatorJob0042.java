package com.niuwa.hadoop.chubao.job;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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

/**读取IndicatorJob0041的输出
 * 输出{"user_id": "xxxx", "total_calls_from_tel_library": 2, "total_diff_num_called_from_tel_library": 4}
 * @author Administrator
 *
 */
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
		
		private Text outValue = new Text();
		private JSONObject outObj = new JSONObject();
		
		public void setup(Context context){
	        /**
	         * 读取cachefiles
	         * 
	         * 官方文档使用这个方法，测试使用上下文也能获取到，还不知道问题所在
	         * URI[] patternsURIs = Job.getInstance(context.getConfiguration()).getCacheFiles();
	         */
	        super.setup(context);
			BufferedReader reader=null;
	        try{
		        
		        Path cacheFilePath= getFilePathWithName(context, ChubaoJobConfig.CONFIG_TEL_LIBARAY_FILE_NAME);
		        reader = new BufferedReader(new FileReader(cacheFilePath.getName().toString()));
		        
		        String tel = null;
	            while((tel=reader.readLine()) != null){
	            	telLibrary.add(tel);
	            }
	                        
	        }catch(Exception e){
	            e.printStackTrace();
	        }finally{
	            try {
					reader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
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
		
		private Path getFilePathWithName(Context context, String fileName) throws IOException{
			
			URI[] paths = context.getCacheFiles();
			for(int i=0; i < paths.length; i++){
				String absPath = paths[i].getPath();
				if(absPath.indexOf(fileName) > -1){
					return new Path(paths[i].getPath());
				}
			}
			
			return null;
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
