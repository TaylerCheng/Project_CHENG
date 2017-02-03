package com.cg.mapreduce.mapredtest;

import com.cg.mapreduce.utils.YarnJobUtil;
import com.cg.mapreduce.wordcount.ExecuteMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import java.net.URI;

public class WordCountTest {

    public static Logger logger = Logger.getLogger(WordCountTest.class);

    public static final String JOB_NAME = "WordCountTest";
    /**
     * LOCAL为本地执行，CLUSTER则提交到YARN集群上执行
     */
    public static final ExecuteMode executeMode = ExecuteMode.CLUSTER;

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length < 3) {
            logger.error("parameters should >= 3");
            System.exit(2);
        }

        // Section 1 init job
        Job job = null;
        if (executeMode.equals(ExecuteMode.CLUSTER)) {
            String classpath = otherArgs[0];
            conf.set("mapred.reduce.tasks", "2");
            //弃用uber模式，重用JVM
            conf.set("mapreduce.job.ubertask.enable","true");
            job = YarnJobUtil.initJob(conf, JOB_NAME, classpath);
        } else {
            //设置为本地运行
            conf.set("mapreduce.framework.name", "local");
            //            conf.set("mapred.reduce.tasks", "2");
            job = Job.getInstance(conf, JOB_NAME);
            job.setJarByClass(WordCountTest.class);
        }

        // Section 2 job config
        job.setMapperClass(TokenizerMapper.class);
        //        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        //        job.setPartitionerClass(MyPartitioner.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(otherArgs[1]));
        Path outputPath = new Path(otherArgs[2]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        TextOutputFormat.setOutputPath(job, outputPath);

        job.addCacheFile(new URI("hdfs://master.hadoop:9000/test/cache/MyCounter.properties"));

        // Section 3 excute job
        job.waitForCompletion(true);
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
        //生成自己的计数器
        Counters counters = job.getCounters();
        Counter counter = counters.findCounter(MyCounter.TEST_COUNTER);
        long value = counter.getValue();
        System.out.println(value);
    }

}

