package com.niuwa.hadoop.chubao.job;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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
import com.niuwa.hadoop.chubao.ChubaoJobConfig;
import com.niuwa.hadoop.chubao.NiuwaMapper;
import com.niuwa.hadoop.chubao.NiuwaReducer;
import com.niuwa.hadoop.chubao.RunParams;
import com.niuwa.hadoop.util.HadoopUtil;


/*
 * 统计客户装载app数量
 * 配置：指定app
 * 输入：app下载历史数据 {"user_id":"xxx","app_display_name":"捕鱼达人"}
 * 输出：{"user_id":"xxxx", "app_num":6}
 * 注意：用户默认app_num=0
 */

public class IndicatorJob007 extends BaseJob{

	private static Set<String> appLibrary = new HashSet<>();
	
	public static class UserIdsMapper extends NiuwaMapper<Object, Text, Text, Text> {
		private Text outKey = new Text();
 		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			JSONObject appLogJson = JSON.parseObject(value.toString());
			
			String userId = appLogJson.getString("user_id");
			outKey.set(userId);
			context.write(outKey, value);
		}
		
	}
	
	
	public static class SumAppsFromAppLibraryReducer extends NiuwaReducer<Text, Text, NullWritable, Text> {
		
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
		        
		        Path cacheFilePath= getFilePathWithName(context, ChubaoJobConfig.CONFIG_APP_LIBARAY_FILE_NAME);
		        reader = new BufferedReader(new InputStreamReader(new FileInputStream(cacheFilePath.getName()),"UTF-8"));
		        
		        String app = null;
	            while((app=reader.readLine()) != null){
	            	appLibrary.add(new String(app.getBytes(), "utf-8"));
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
			
			Set<String> downloadedAppSet = new HashSet<String>();
			
			for(Text val: values){
				JSONObject json = JSONObject.parseObject(val.toString());
				if(appLibrary.contains(json.getString("app_display_name"))){
					downloadedAppSet.add(json.getString("app_display_name"));
				}
			}
			
			outObj.put("user_id", key.toString());
			outObj.put("total_apps_from_app_library", downloadedAppSet.size());
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
        
        job.setMapperClass(IndicatorJob007.UserIdsMapper.class);
        job.setReducerClass(IndicatorJob007.SumAppsFromAppLibraryReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
		
		job.addCacheFile(ChubaoJobConfig.getConfigPath(ChubaoJobConfig.CONFIG_APP_LIBARAY_FILE_NAME).toUri());
		
        // 输入路径
        FileInputFormat.addInputPath(job, ChubaoJobConfig.getInputPath(ChubaoJobConfig.INPUT_APP));
        // 输出路径
        FileOutputFormat.setOutputPath(job, tempPaths.get(IndicatorJob007.class.getName()) );
        // 删除原有的输出
        HadoopUtil.deleteOutputFile(tempPaths.get(IndicatorJob007.class.getName()));
        
	}

	
}
