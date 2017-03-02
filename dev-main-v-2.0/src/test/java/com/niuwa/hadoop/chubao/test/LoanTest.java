package com.niuwa.hadoop.chubao.test;

import java.net.URI;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.niuwa.hadoop.chubao.job.IndicatorJob006;
import com.niuwa.hadoop.chubao.job.IndicatorJob006.RationMapper;
import com.niuwa.hadoop.util.DateUtil;
import com.niuwa.hadoop.util.HadoopUtil;

public class LoanTest {

    public static void main(String[] args) throws Exception {
        HadoopUtil.isWinOrLiux();
        Configuration conf = new Configuration();
        args = new String[] { "hdfs://ns1:9000/user/root/chubao/input/loan",
                "hdfs://ns1:9000/user/root/chubao/temp/" + DateUtil.format(new Date()) + "/large" };// +new
                                                                                                                    // Date().getTime()};
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: large_condition1_2 <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "large_condition1_2");

        job.setJarByClass(IndicatorJob006.class);
        job.setMapperClass(RationMapper.class);
//        job.setCombinerClass(LargeCombiner.class);
//        job.setReducerClass(LargeReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // 删除原有的输出
        deleteOutputFile(otherArgs[1], otherArgs[0]);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    static void deleteOutputFile(String path, String inputDir) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(inputDir), conf);
        if (fs.exists(new Path(path))) {
            fs.delete(new Path(path), true);
        }
    }
}
