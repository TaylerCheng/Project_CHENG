package com.cg.mapreduce.mapredtest;

import com.cg.mapreduce.mapredtest.io.MyPair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author： Cheng Guang
 * @date： 2017/3/29.
 */
public class JoinReduce extends Reducer<MyPair, Text, NullWritable, Text> {
    private Text outValue = new Text();

    public void reduce(MyPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> cacheData = new ArrayList<String>();
        for (Text value : values) {
            String keyStr = key.toString();
            if (keyStr.startsWith("A")) {
                cacheData.add(value.toString());
            } else {
                for (String data : cacheData) {
                    outValue.set(data + "\t" + value.toString());
                    System.out.println(outValue.toString());
                    context.write(NullWritable.get(), outValue);
                }
            }
        }
    }

}
