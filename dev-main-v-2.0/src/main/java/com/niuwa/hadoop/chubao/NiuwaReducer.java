package com.niuwa.hadoop.chubao;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.niuwa.hadoop.chubao.utils.ChubaoDateUtil;

public class NiuwaReducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>{
	private static final Logger log= LoggerFactory.getLogger(NiuwaReducer.class);
	
	public void setup(Context context){
		try{
	        URI[] paths =context.getCacheFiles();
	        log.info("try to read global config file...");
	        Path cacheFilePath= new Path(paths[0].getPath());
	        BufferedReader reader= new BufferedReader(new FileReader(cacheFilePath.getName().toString()));
	        StringBuffer content= new StringBuffer();
	        String str= null;

	        while((str= reader.readLine())!=null){
	           	content.append(str);
	           }
	        JSONObject conf= JSONObject.parseObject(content.toString());
	        
	        // 设置全局的变量
	        ChubaoJobConfig.setRootPath(conf.getString("rootPath"));
	        ChubaoDateUtil.setDataLastedTime(conf.getLong("dataLastedTime"));
	        ChubaoJobConfig.setDebugMode(conf.getBooleanValue("isDebug"));
	        
		}catch(Exception e){
			log.error("Read global config file failed!");
		}
		finally{
		}
	}	
}
