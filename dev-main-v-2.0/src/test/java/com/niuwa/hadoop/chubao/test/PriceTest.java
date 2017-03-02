package com.niuwa.hadoop.chubao.test;

import java.net.URI;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.niuwa.hadoop.chubao.job.IndicatorJob005;
import com.niuwa.hadoop.chubao.job.IndicatorJob005.PriceRuleMapper;
import com.niuwa.hadoop.util.DateUtil;
import com.niuwa.hadoop.util.HadoopUtil;

public class PriceTest {
    public static void main(String[] args) throws Exception {
        HadoopUtil.isWinOrLiux();
        Configuration conf = new Configuration();
        args=new String[]{"hdfs://ns1:9000/user/root/chubao/input/user_info",
                "hdfs://ns1:9000/user/root/chubao/temp/"+DateUtil.format(new Date())+"/rank_addr"};//+new Date().getTime()};
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: userinfo <in> <out>");
            System.exit(2);
        }
        Job job =Job.getInstance(conf, "userinfo_addr");
        /**
         * 设置需要缓存的小文件
         */
        job.addCacheFile(new Path("hdfs://ns1:9000/user/root/chubao/input/STATIC/static-mobile-tier.txt").toUri());
        
        job.setJarByClass(IndicatorJob005.class);
        job.setMapperClass(PriceRuleMapper.class);
//      job.setCombinerClass(PriceRuleReducer.class);
//      job.setReducerClass(PriceRuleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        //删除原有的输出
        deleteOutputFile(otherArgs[1],otherArgs[0]);
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
