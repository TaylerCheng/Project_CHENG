package com.cg.hdfs.squencefile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SquenFileUtils {

	private final static String[] STRS = new String[] { "one", "two", "three",
			"four", "five" };
	private static Configuration conf = new Configuration();

	public static void main(String[] args) throws Exception {
		write();
	}

	private static void write() throws Exception {
		Path seqFile = new Path("C:/Users/cheng/Desktop/seqFile.txt");
		FileSystem fs = FileSystem.getLocal(conf);
		FSDataOutputStream fos = fs.create(seqFile);
		IntWritable key = new IntWritable();
		Text value = new Text();
		for (int i = 0; i < 100; i++) {
			key.set(i);
			value.set(STRS[i % 5]);
            fos.writeChars(key.toString());
            fos.writeChars(value.toString());
		}
		fos.close();
		// SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
		// seqFile,IntWritable.class, Text.class);
		// for (int i = 0; i < 100; i++) {
		// key.set(i);
		// value.set(STRS[i%5]);
		// writer.append(key, value);
		// }
		// writer.close();
	}
}
