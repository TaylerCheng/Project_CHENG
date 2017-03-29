package com.cg.mapreduce.wordcount;

import com.cg.mapreduce.utils.YarnJobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.File;
import java.net.URI;

public class WordCount extends Configured implements Tool {

    public static Logger logger = Logger.getLogger(WordCount.class);

    public static final String JOB_NAME = "WordCountTest";
    // LOCAL为本地执行，CLUSTER则提交到YARN集群上执行
    public static final ExecuteMode executeMode = ExecuteMode.LOCAL;

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        int success = ToolRunner.run(new WordCount(), args);
        long endTime = System.currentTimeMillis();
        System.out.println("耗时：" + (endTime - startTime) / 1000);
        System.exit(success);
    }

    @Override
    public int run(String[] otherArgs) throws Exception {
        if (otherArgs.length < 2) {
            logger.error("parameters should >= 2");
            System.exit(2);
        }

        // Section 1 init job
        Configuration conf = getConf();
        Job job = null;
        if (executeMode.equals(ExecuteMode.CLUSTER)) {
            conf.set("mapreduce.job.reduces", "3");
            job = Job.getInstance(conf, JOB_NAME);
            String classpath = otherArgs[2];
            File jarfile = YarnJobUtil.getJobJarFile(classpath);
            if (jarfile != null) {
                logger.warn("初始化jar包成功");
                job.setJar(jarfile.toString());
            } else {
                logger.warn("初始化jar包失败，改为本地运行");
                conf.set("mapreduce.framework.name", "local");
                job.setJarByClass(WordCount.class);
            }
        } else {
            conf.set("mapreduce.framework.name", "local");
            job = Job.getInstance(conf, JOB_NAME);
            job.setJarByClass(WordCount.class);
        }

        // Section 2 job config
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        Path outputPath = new Path(otherArgs[1]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        TextOutputFormat.setOutputPath(job, outputPath);
        job.setNumReduceTasks(4);

        // Section 3 excute job
        return job.waitForCompletion(true) ? 0 : 1;
    }

}

