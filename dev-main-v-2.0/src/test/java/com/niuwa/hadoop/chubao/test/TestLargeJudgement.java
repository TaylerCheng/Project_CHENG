package com.niuwa.hadoop.chubao.test;

import java.net.URI;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.niuwa.hadoop.chubao.job.IndicatorJob006;
import com.niuwa.hadoop.chubao.job.IndicatorJob006.RationMapper;
import com.niuwa.hadoop.chubao.job.LargeIndicatorJob001;
import com.niuwa.hadoop.chubao.job.LargeIndicatorJob001.LargeMapper;
import com.niuwa.hadoop.chubao.job.LargeIndicatorJob001.LargeReducer;
import com.niuwa.hadoop.chubao.job.LargeIndicatorJob003;
import com.niuwa.hadoop.chubao.job.LargeIndicatorJob003.LargePriceRuleMapper;
import com.niuwa.hadoop.chubao.job.LargeJugementJob;
import com.niuwa.hadoop.util.DateUtil;
import com.niuwa.hadoop.util.HadoopUtil;

public class TestLargeJudgement {
    public static void main(String[] args) throws Exception {
        String path = "hdfs://ns1:9000/user/root";
        HadoopUtil.isWinOrLiux();
        Configuration conf = new Configuration();
        if (args.length != 0) {
            path = args[0];
        }
        String[] inputPath = new String[] {
                path+"/chubao/input/user_info",
                path+"/chubao/input/STATIC/static-mobile-tier.txt",
                path+"/chubao/input/loan",
        };
        
        String[] tempPath=new String[]{
                path+"/chubao/temp/"+DateUtil.format(new Date())
                +"/large_rank_addr",
                path+"/chubao/temp/"+DateUtil.format(new Date())
                +"/largeCondition1",
                path+"/chubao/temp/"+DateUtil.format(new Date())
                +"/loanRation"
        };
        
        String[] outputPath=new String[]{
                path+"/chubao/output/"+DateUtil.format(new Date())
                +"/large"
        };
        
        
        //大额白名单规则1_2
        Job job1 = Job.getInstance(conf,"large_loan_rule");
        
        job1.setJarByClass(LargeIndicatorJob001.class);
        job1.setMapperClass(LargeMapper.class);
        job1.setReducerClass(LargeReducer.class);
        //job1.setCombinerClass(LargeIndicatorJob001.LargeCombiner.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(inputPath[2]));
        // 删除原有的输出
        deleteOutputFile(tempPath[1]);
        FileOutputFormat.setOutputPath(job1, new Path(tempPath[1]));
        
        //定额参数
        Job job2 = Job.getInstance(conf,"ration_param");
        
        job2.setJarByClass(IndicatorJob006.class);
        job2.setMapperClass(RationMapper.class);
//        job2.setReducerClass(RationReducer.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(inputPath[2]));
        // 删除原有的输出
        deleteOutputFile(tempPath[2]);
        FileOutputFormat.setOutputPath(job2, new Path(tempPath[2]));
        
        //大额定价
        Job job3 = Job.getInstance(conf,"large_price");
        /**
         * 设置需要缓存的小文件
         */
        job3.addCacheFile(new Path(inputPath[1]).toUri());
        job3.setJarByClass(LargeIndicatorJob003.class);
        job3.setMapperClass(LargePriceRuleMapper.class);
        job3.setOutputKeyClass(NullWritable.class);
        job3.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job3, new Path(inputPath[0]));
        // 删除原有的输出
        deleteOutputFile(tempPath[0]);
        FileOutputFormat.setOutputPath(job3, new Path(tempPath[0]));
        
        // job4  合并技术指标
        Job job4 = Job.getInstance(conf, "join_profile");
        job4.setJarByClass(LargeJugementJob.class);
        job4.setMapperClass(LargeJugementJob.MapSevralTemp.class);
        //job4.setCombinerClass(ChubaoCondition3.CombinerSumReducer.class);
        job4.setReducerClass(LargeJugementJob.ReduceObj.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);   
//   
        FileInputFormat.addInputPath(job4, new Path(tempPath[0]));
        FileInputFormat.addInputPath(job4, new Path(tempPath[1]));
        FileInputFormat.addInputPath(job4, new Path(tempPath[2]));
        FileInputFormat.addInputPath(job4, new Path(inputPath[2]));
        // 删除原有的输出
        deleteOutputFile(outputPath[0]);
        FileOutputFormat.setOutputPath(job4, new Path(outputPath[0]));
        
        // 创建ControlledJob
        ControlledJob controlledjob1 = new ControlledJob(job1.getConfiguration());
        ControlledJob controlledjob2 = new ControlledJob(job2.getConfiguration());
        ControlledJob controlledjob3 = new ControlledJob(job3.getConfiguration());
        ControlledJob controlledjob4 = new ControlledJob(job4.getConfiguration());
      
        // 添加依赖
        controlledjob2.addDependingJob(controlledjob1);
        controlledjob4.addDependingJob(controlledjob2);
        controlledjob4.addDependingJob(controlledjob3);
        
        // 创建JobControl
        JobControl jobControl = new JobControl("largeJobGroup");
        jobControl.addJob(controlledjob1);
        jobControl.addJob(controlledjob2);
        jobControl.addJob(controlledjob3);
        jobControl.addJob(controlledjob4);
        
        // 启动任务
        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();
        while (true) {
            if (jobControl.allFinished()) {
                System.out.println(jobControl.getSuccessfulJobList());
                jobControl.stop();
                break;
            }
        }
    }

    static void deleteOutputFile(String path) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(path), conf);
        if (fs.exists(new Path(path))) {
            fs.delete(new Path(path), true);
        }
    }
}
