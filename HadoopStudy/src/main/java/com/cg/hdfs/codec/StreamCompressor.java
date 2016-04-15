package com.cg.hdfs.codec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class StreamCompressor {

	private static Configuration conf = new Configuration();

	public static void main(String[] args) throws Exception {
		compress();
		// decompress();
	}

	private static void decompress() throws Exception {
		// 待解压的文件
		Path inPath = new Path("C:/Users/cheng/Desktop/file_out.txt.deflate");

		// 通过文件后缀名生成相应压缩工具
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		CompressionCodec codec = factory.getCodec(inPath);

		// 读入压缩文件
		FileSystem fs = FileSystem.getLocal(conf);
		FSDataInputStream fis = fs.open(inPath);
		CompressionInputStream cis = codec.createInputStream(fis);

		// 解压后的文件
		Path outPath = new Path("C:/Users/cheng/Desktop/file_out.txt");
		FSDataOutputStream fos = fs.create(outPath);
		// FileOutputStream fos = new FileOutputStream(
		// "C:/Users/cheng/Desktop/file_out.txt");

		IOUtils.copyBytes(cis, fos, 4096, false);
		fos.close();
		cis.close();
		System.out.println("yes");

	}

	private static void compress() throws Exception {
		// 生成压缩工具
		Class<?> c = Class
				.forName("org.apache.hadoop.io.compress.DeflateCodec");
		CompressionCodec codec = (CompressionCodec) ReflectionUtils
				.newInstance(c, conf);

		// 待压缩的文件
		FileSystem fs = FileSystem.getLocal(conf);
		Path inPath = new Path("C:/Users/cheng/Desktop/file_in.txt");
		FSDataInputStream fis = fs.open(inPath);

		// 压缩后的文件
		Path outPath = new Path("C:/Users/cheng/Desktop/file_out.txt");
		FSDataOutputStream fos = fs.create(outPath);
		CompressionOutputStream cos = codec.createOutputStream(fos);

		IOUtils.copyBytes(fis, cos, 4096, false);
		cos.finish();
		fos.close();
		fis.close();
		System.out.println("yes");
	}

}
