package com.niuwa.hadoop.jobs.sample;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.niuwa.hadoop.util.HadoopUtil;

/**
 * 使用distributed cache file加载小文件
 * 
 * @author Administrator
 *
 */
public class SimpleMapJoin {
	private static final Logger log= LoggerFactory.getLogger(SimpleMapJoin.class);

	public static class Map1 extends Mapper<Object, Text, Text, Text>{
		private Map<String, String> childAgeMap= new HashMap<String, String>();
		
		public void  setup(Context context) throws IOException{
			/**
			 * 读取cachefiles
			 * 
			 * 官方文档使用这个方法，测试使用上下文也能获取到，还不知道问题所在
			 * URI[] patternsURIs = Job.getInstance(context.getConfiguration()).getCacheFiles();
			 */
			
			URI[] paths =context.getCacheFiles();
			//Path chileAge= new Path(paths[0]);
			log.info("[ cached file number ]{}", paths.length);
			Path cacheFilePath= new Path(paths[0].getPath());
			BufferedReader reader= new BufferedReader(new FileReader(cacheFilePath.getName().toString()));
			String str= null;
			try{
				while((str= reader.readLine())!=null){
					String[] spilts= str.split("\\s");
					childAgeMap.put(spilts[0], spilts[1]);
				}
			}catch(Exception e){
				e.printStackTrace();
			}finally{
				reader.close();
			}
		}
		
		public void map(Object key, Text record, Context context){
			 String[] values = record.toString().split("\\s+"); 

			 try {
				context.write(new Text(values[0]), new Text(record.toString()+"\t"+ this.childAgeMap.get(values[0])));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		HadoopUtil.isWinOrLiux();
		Configuration conf = new Configuration();
		args= new String[]{"hdfs://ns1:9000/user/root/demo/input/child-parent.txt"
				,"hdfs://ns1:9000/user/root/demo/output/wangjiazhi/mapsidejoin"};
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();  

		deleteOutputFile(otherArgs[1],otherArgs[0]);
		
		Job job =Job.getInstance(conf, "map side join");
		
		/**
		 * 设置需要缓存的小文件
		 */
		job.addCacheFile(new Path("hdfs://ns1:9000/user/root/demo/input/child-age.txt").toUri());;
		
		job.setJarByClass(SimpleMapJoin.class);
		job.setMapperClass(SimpleMapJoin.Map1.class);
		//job.setReducerClass(SimpleJoin.SimpleJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new  org.apache.hadoop.fs.Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new  org.apache.hadoop.fs.Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);		
		
	}
	static void deleteOutputFile(String path,String inputDir) throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(inputDir),conf);
        if(fs.exists(new  org.apache.hadoop.fs.Path(path))){
        	fs.delete(new  org.apache.hadoop.fs.Path(path), true);
        }
    }	
}
