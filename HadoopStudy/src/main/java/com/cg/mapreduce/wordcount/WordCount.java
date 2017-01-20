package com.cg.mapreduce.wordcount;

import java.io.File;
import java.io.IOException;

import com.cg.utils.YarnJobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.cg.utils.EJob;
import org.apache.log4j.Logger;

public class WordCount {

    public static Logger logger = Logger.getLogger(WordCount.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("parameters should >= 2");
            System.exit(2);
        }

        //LOCAL为本地执行，CLUSTER则提交到YARN集群上执行
        ExecuteMode executeMode = ExecuteMode.LOCAL;

        // Section 1
        Job job = null;
        if (executeMode.equals(ExecuteMode.CLUSTER)) {
            String outPutPath = "E:\\gitspace\\Project_CHENG\\HadoopStudy\\target\\classes";
            conf.set("mapred.reduce.tasks", "2");
            job = YarnJobUtil.initJob(conf, outPutPath);
            if (job == null) {
                logger.error("yarn-mode init failed!");
                System.exit(0);
            }
        } else {
            job = new Job(conf, "wordcount");
            job.setJarByClass(WordCount.class);
        }

        // Section 2
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        // Section 3
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        Path outputPath = new Path(otherArgs[1]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath);
        }
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, outputPath);

        // Section 4
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

