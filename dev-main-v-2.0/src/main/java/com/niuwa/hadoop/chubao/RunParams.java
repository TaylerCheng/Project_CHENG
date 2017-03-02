package com.niuwa.hadoop.chubao;

import com.niuwa.hadoop.util.DateUtil;

/*
 * 对应于命令行参数，详见ChubaoCommandLine Option
 */
public class RunParams {

	private boolean isRunAtRemote;
	private String jarPath;
	private int defaultReducerTaskNum;
	private String rootPath;
	private String time;
	private boolean debug;
	private boolean isSmall;
	
	public RunParams() {
		isRunAtRemote = false;
		jarPath = null;
		defaultReducerTaskNum = 1;
		rootPath = null;
		debug = false;
		time = null;
		isSmall = true;
	}
	
	public boolean isRunAtRemote() {
		return isRunAtRemote;
	}
	public void setRunAtRemote(boolean isRunAtRemote) {
		this.isRunAtRemote = isRunAtRemote;
	}
	public String getJarPath() {
		return jarPath;
	}
	public void setJarPath(String jarPath) {
		this.jarPath = jarPath;
	}
	public int getDefaultReducerTaskNum() {
		return defaultReducerTaskNum;
	}
	public void setDefaultReducerTaskNum(int defaultReducerTaskNum) {
		this.defaultReducerTaskNum = defaultReducerTaskNum;
	}
	public String getRootPath() {
		return rootPath;
	}
	public void setRootPath(String rootPath) {
		this.rootPath = rootPath;
	}
	public String getTime() {
		return time;
	}
	public void setTime(String time) {
		try{
			DateUtil.parse(time);
			this.time = time;
		} catch(java.text.ParseException ex){
			throw new IllegalArgumentException("非法时间");
		}
	}
	public boolean isDebug() {
		return debug;
	}
	public void setDebug(boolean debug) {
		this.debug = debug;
	}
	public boolean isSmall() {
		return isSmall;
	}

	public void setSmall(boolean isSmall) {
		this.isSmall = isSmall;
	}
}
