package com.cg.mapreduce.mapredtest;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.StringTokenizer;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    private Text word = new Text();
    private Text cache = new Text();

    @Override
    public void setup(Context context) throws IOException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles.length == 0) {
            throw new FileNotFoundException();
        }
        URI uri = cacheFiles[0];
        FileSystem fileSystem = FileSystem.get(uri, context.getConfiguration());
        InputStream in = null;
        ByteArrayOutputStream out = null;
        try {
            in = fileSystem.open(new Path(uri));
            out = new ByteArrayOutputStream();
            IOUtils.copyBytes(in, out, 1024);
            cache = new Text(out.toString());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
        //Path path = new Path(cacheFiles[0]);
    }

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        System.out.println("+++++++++++++++++++++" + cache);
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.getCounter("Cheng Guang", word.toString()).increment(1);
            context.getCounter(MyCounter.TEST_COUNTER).increment(1);
            context.write(word, one);
        }
    }

}