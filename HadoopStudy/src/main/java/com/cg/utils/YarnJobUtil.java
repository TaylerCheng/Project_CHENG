package com.cg.utils;

import com.cg.mapreduce.wordcount.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

/**
 * Created by Cheng Guang on 2016/9/14.
 */
public class YarnJobUtil {

    public static Logger logger = Logger.getLogger(YarnJobUtil.class);

    public static Job initJob(Configuration conf, String outPutPath) {
        conf.set("HADOOP_USER_NAME", "hadoop");
        conf.set("mapred.reduce.tasks", "2");
        Job job = null;
        try {
            File jarFile = EJob.createTempJar(outPutPath);
            ClassLoader classLoader = EJob.getClassLoader();
            Thread.currentThread().setContextClassLoader(classLoader);
            job = new Job(conf, "wordcount");
            ((JobConf) job.getConfiguration()).setJar(jarFile.toString());
        } catch (IOException e) {
            logger.info("Init job on yarn failed.");
        }
        return job;
    }
}
