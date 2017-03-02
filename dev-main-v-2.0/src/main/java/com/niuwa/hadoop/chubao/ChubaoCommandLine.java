package com.niuwa.hadoop.chubao;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

public class ChubaoCommandLine {

	private static final String help = "usage: [-r string] [-t string] [-d] [-nr int] [-j string] [-L]";

	public static boolean parse(String[] args, Options options, RunParams params) throws Exception {
		
		CommandLineParser parser = new PosixParser();
		
		try {
			CommandLine cmdLine = parser.parse(options, args);
			
			if(cmdLine.hasOption("h")){
	        	showTips(help, options);
	        	return false;
	        }else{
	        	if(!cmdLine.hasOption("r")){
	        		showTips("root path must be specified", options);
	        		return false;
	        	}else{
	        		params.setRootPath(cmdLine.getOptionValue("r"));
	        		if(cmdLine.hasOption("t")){
	        			params.setTime(cmdLine.getOptionValue("t"));
	        		}
	        		if(cmdLine.hasOption("L")){
	        			params.setSmall(false);
	        		}
	        		if(cmdLine.hasOption("d")){
	        			params.setDebug(true);
	        		}
	        		if(cmdLine.hasOption("nr")){
	        			params.setDefaultReducerTaskNum(Integer.valueOf(cmdLine.getOptionValue("nr")));
	        		}
	        		if(cmdLine.hasOption("j")){
	        			params.setRunAtRemote(true);
	        			params.setJarPath(cmdLine.getOptionValue("j"));
	        		}
	        		return true;
	        	}
	        }
		} catch (Exception e) {
			showTips(help, options);
			throw new Exception(e);
		}
	}
	
	/*
	 * -r hdfs://ns1:9000/user/root/demo-2-test -t 2016-08-01 -nr 3 -d
	 */
	public static Options buildOptions() {
		Option helpOption = new Option("h", false, "show help");
		Option rootPathOption = new Option("r", true, "root path, it must be specified");
		Option timeOption = new Option("t", true, "time to run script, yyyy-MM-dd, default is the first day of current month");
		Option debugOption = new Option("d", false, "debug mode. If set, jobs will run in debug mode");
		Option reducerNumberOption = new Option("nr", true, "number of reduce tasks. default is 1");
		Option jarPathOption = new Option("j", true, "the local jar path. If given, jobs will be executed in remote clusters");
		Option largeOption = new Option("L", false, "large white list. If set, large white list will be calculated. default, small white list will be calculated");
		
		Options options = new Options();
	    options.addOption(helpOption);
	    options.addOption(rootPathOption);
	    options.addOption(timeOption);
	    options.addOption(debugOption);
	    options.addOption(reducerNumberOption);
	    options.addOption(jarPathOption);
	    options.addOption(largeOption);
		return options;
	}
	
	private static void showTips(String usage, Options options){
		HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(usage, options);
	}
	
}
