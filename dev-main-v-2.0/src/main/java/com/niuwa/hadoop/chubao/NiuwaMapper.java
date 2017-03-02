package com.niuwa.hadoop.chubao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.niuwa.hadoop.chubao.utils.ChubaoDateUtil;

public class NiuwaMapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	private static final Logger log= LoggerFactory.getLogger(NiuwaMapper.class);
	
	//完成全局变量的设置，主要解决不同节点之间无法共享信息的问题
	public void setup(Context context){
		try{
	        Configuration conf = context.getConfiguration();
	        // 设置全局的变量
	        ChubaoJobConfig.setRootPath(conf.get("rootPath"));
	        ChubaoDateUtil.setDataLastedTime(Long.valueOf(conf.get("dataLastedTime")));
	        ChubaoJobConfig.setDebugMode(Boolean.valueOf(conf.get("isDebug")));
	        
		}catch(Exception e){
			log.error("Read global config file failed!");
		}
	}
}
