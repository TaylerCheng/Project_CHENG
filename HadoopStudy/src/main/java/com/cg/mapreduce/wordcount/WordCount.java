package com.cg.mapreduce.wordcount;

import com.cg.utils.YarnJobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.log4j.Logger;

public class WordCount {

    public static Logger logger = Logger.getLogger(WordCount.class);

    public static final String JOB_NAME = "WordCount";
    /**
     *  LOCAL为本地执行，CLUSTER则提交到YARN集群上执行
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
            job = YarnJobUtil.initJob(conf, JOB_NAME, classpath);
        } else {
            job = Job.getInstance(conf, JOB_NAME);
            job.setJarByClass(WordCount.class);
        }

        // Section 2 job config
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setPartitionerClass(MyPartitioner.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(otherArgs[1]));
        Path outputPath = new Path(otherArgs[2]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath,true);
        }
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        TextOutputFormat.setOutputPath(job, outputPath);

        // Section 3 excute job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

