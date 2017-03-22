package com.niuwa.hadoop.chubao;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.niuwa.hadoop.chubao.utils.ChubaoDateUtil;

public class NiuwaReducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>{
	private static final Logger log= LoggerFactory.getLogger(NiuwaReducer.class);
	
	public void setup(Context context) {
		try {
			Configuration conf = context.getConfiguration();
			// 设置全局的变量
			ChubaoJobConfig.setRootPath(conf.get("rootPath"));
			ChubaoDateUtil.setDataLastedTime(Long.valueOf(conf.get("dataLastedTime")));
			ChubaoJobConfig.setDebugMode(Boolean.valueOf(conf.get("isDebug")));
		} catch (Exception e) {
			log.error("Read global config file failed!", e);
		} finally {
		}
	}	
}
