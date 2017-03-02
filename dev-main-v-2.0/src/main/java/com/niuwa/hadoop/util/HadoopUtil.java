package com.niuwa.hadoop.util;

import java.io.File;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopUtil {
	/**
	 * windows访问hadoop环境需要以下代码配置
	 */
	public static void isWinOrLiux() {
		// 判断是window还是linux
		Properties props = System.getProperties(); // 系统属性
		String osName = props.getProperty("os.name");
		if (osName.indexOf("Win") > -1) { // windows系统【特殊需要wintuils方便windows系统】
			File workaround = new File(".");
			System.getProperties().put("hadoop.home.dir",
					workaround.getAbsolutePath()+"/tools");
		}
	}
		
	/**
	 * 删除HDFS上的文件
	 * 
	 * @param path
	 * @throws Exception
	 */
	public static void deleteOutputFile(Path path) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(path.toUri(), conf);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
	}
}
