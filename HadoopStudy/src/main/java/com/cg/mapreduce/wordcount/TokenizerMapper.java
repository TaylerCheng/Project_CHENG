package com.cg.mapreduce.wordcount;

import java.io.*;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	public static Logger logger = Logger.getLogger(TokenizerMapper.class);

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		File file = new File("./CHENGTEST");
		InputStream in = null;
		try {
			in = new FileInputStream(file);
			IOUtils.copyBytes(in, System.out, 1024);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}
	}

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			context.write(word, one);
		}
	}
}