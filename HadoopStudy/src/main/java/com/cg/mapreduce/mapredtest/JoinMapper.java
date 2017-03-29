package com.cg.mapreduce.mapredtest;

import com.cg.mapreduce.mapredtest.io.MyPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author： Cheng Guang
 * @date： 2017/3/29.
 */
public class JoinMapper extends Mapper<LongWritable, Text, MyPair, Text> {

    private MyPair id = new MyPair();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String inputPath = ((FileSplit) context.getInputSplit()).getPath().toString();
        StringTokenizer itr = new StringTokenizer(value.toString());
        if (inputPath.contains("name")) {
            id.set("A" + itr.nextToken());
            context.write(id, value);
        }
        if (inputPath.contains("age")) {
            id.set("B" + itr.nextToken());
            context.write(id, value);
        }

    }
}
