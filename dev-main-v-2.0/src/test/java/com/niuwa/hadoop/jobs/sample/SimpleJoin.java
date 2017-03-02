package com.niuwa.hadoop.jobs.sample;

/**
 * simple join
 * 
 * 在ruduce阶段进行join，
 * 要点：
 * 1、按链接主键map阶段拆分左右表并标记
 * 2、reduce阶段进行链接
 * 3、Combiner为空
 * 
 * 
 */
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.niuwa.hadoop.util.HadoopUtil;

public class SimpleJoin {
	public static class SimpleJoinMap extends Mapper<Object,Text,Text,Text>{
		public static int time= 0;
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
//			StringTokenizer str= new StringTokenizer(value.toString());
			
			String childName= new String();
			String parentName= new String();
			String relationType= new String();
			String line= value.toString();
			
			int i=0;
			while(line.charAt(i) != ' '){
				i++;
			}
			
			String[] values= {line.substring(0,i), line.substring(i+1)};
			if(values[0].compareTo("child") != 0){
				childName= values[0];
				parentName= values[1];
				relationType= "1"; // left table
				context.write(new Text(values[1]), new Text(relationType+"+"+childName+"+"+parentName));
			
				relationType= "2"; // right table
				context.write(new Text(values[0]), new Text(relationType+"+"+childName+"+"+parentName));
			}
		}
	}
	
	public static class SimpleJoinReducer extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException{
			if(SimpleJoinMap.time == 0){
				context.write(new Text("grandchild"), new Text("grandparent"));
				SimpleJoinMap.time= SimpleJoinMap.time+1;
			}
			
			int grandChildNum= 0;
			String grandChild[]= new String[10];
			int grandParentNum= 0;
			String grandParent[]= new String[10];		
			
			Iterator<Text> ite= values.iterator();
			while(ite.hasNext()){
				String record= ite.next().toString();
				int len= record.length();
				
				int i=2;
				if(len == 0) continue;
				
				char relationType= record.charAt(0);
				String childName= new String();
				String parentName= new String();
				
				while(record.charAt(i)!='+'){
					childName= childName+record.charAt(i);
					i++;
				}
				i++;
				while(i<len){
					parentName= parentName+ record.charAt(i);
					i++;
				}
				
				if(relationType=='1'){
					grandChild[grandChildNum]= childName;
					grandChildNum++;
				}else{
					grandParent[grandParentNum]= parentName;
					grandParentNum++;
				}
			}
			
			if(grandChild.length != 0 && grandParent.length != 0 ){
				for(int i=0; i<grandChild.length; i++){
					for(int j=0; j<grandParent.length; j++){
						if(grandChild[i] != null && grandParent[j] != null)
						context.write(new Text(grandChild[i]), new Text(grandParent[j]));
					}
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		HadoopUtil.isWinOrLiux();
		Configuration conf= new Configuration();
		
		args =new String[]{"hdfs://192.168.101.219:9000/user/root/demo/input/child-parent.txt"
				,"hdfs://192.168.101.219:9000/user/root/demo/output/wangjiazhi/join"};
	
		String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		
		deleteOutputFile(otherArgs[1],otherArgs[0]);
		
		Job job =Job.getInstance(conf, "simple join");
		job.setJarByClass(SimpleJoin.class);
		job.setMapperClass(SimpleJoin.SimpleJoinMap.class);
		job.setReducerClass(SimpleJoin.SimpleJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}
	
	static void deleteOutputFile(String path,String inputDir) throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(inputDir),conf);
        if(fs.exists(new Path(path))){
        	fs.delete(new Path(path), true);
        }
    }
}
