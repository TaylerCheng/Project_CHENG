package com.cg.hdfs;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class FileSystemTest {

    private static Configuration conf = new Configuration();
	
    public static Configuration getConfig() {
		return conf;
	}
    
	public static void main(String[] args) {
		// test();
		// test2();
		//fileRead1();
		// fileRead3();
		fileWrite1();
		//fileRead2();
		// mkdir();
		// listFile();
		// compresssor();
		// squenceFileWrite();
		//squenceFileRead();
		//mapFileWrite();
	}

	private static void mapFileWrite() {
		String uri = "hdfs://192.168.99.156:9000/output/test.map";
		Configuration conf = new Configuration();
		MapFile.Writer writer = null;
		try {
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			Path path = new Path(uri);
			IntWritable key = new IntWritable();
			Text value = new Text();
			writer = new MapFile.Writer(conf,fs, uri, key.getClass(),
					value.getClass());
			for (int i = 0; i < 1024; i++) {
				key.set(i);
				value.set("data"+i);
				writer.append(key, value);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(writer);
		}
	}

	private static void squenceFileRead() {
		String uri = "hdfs://192.168.99.156:9000/output/test.seq";
		Configuration conf = new Configuration();
		SequenceFile.Reader reader = null;
		try {
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			Path path = new Path(uri);
			reader = new SequenceFile.Reader(fs, path,conf);
            Writable key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
            reader.sync(1);
            System.out.println(reader.getPosition());
            while(reader.next(key, value)){
            	System.out.print(key+"\t"+value);
            	System.out.println("\t"+reader.getPosition());
            }
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
		}
	}

	private static void squenceFileWrite() {
		String[] data = new String[] { "first", "second", "third", "fourth",
				"fifth" };
		String uri = "hdfs://192.168.99.156:9000/output/test.seq";
		Configuration conf = new Configuration();
		SequenceFile.Writer writer = null;

		try {
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			Path path = new Path(uri);
			IntWritable key = new IntWritable();
			Text value = new Text();
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(),
					value.getClass());
			for (int i = 0; i < 100; i++) {
				key.set(i + 1);
				value.set(data[i%5]);
				writer.append(key, value);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(writer);
		}
	}

	private static void test2() {
		Text text = new Text("cheng");
		System.out.println(text.toString());
		// IntWritable num = new IntWritable(30);
		// byte[] bytes= serialize(num);
		// System.out.println(StringUtils.byteToHexString(bytes));
		// BytesWritable b = new BytesWritable(new byte[]{5,2});
		// System.out.println(b.getLength());
		// byte[] bytes= serialize(b);
		// System.out.println(StringUtils.byteToHexString(bytes));
	}

	public static byte[] serialize(Writable writable) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(out);
		try {
			writable.write(dataOut);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				dataOut.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return out.toByteArray();
	}

	private static void fileRead3() {
		InputStream in = null;
		try {
			String path = "hdfs://192.168.99.156:9000/test/000000_0.deflate";
			Path file = new Path(path);
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(path), conf);
			CompressionCodecFactory factory = new CompressionCodecFactory(conf);
			CompressionCodec codec = factory.getCodec(file);
			in = codec.createInputStream(fs.open(file));
			IOUtils.copyBytes(in, System.out, 4096, false);
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			IOUtils.closeStream(in);
		}
	}

	private static void compresssor() {
		String codecMethod = "org.apache.hadoop.io.compress.GzipCodec";
		Configuration conf = new Configuration();
		try {
			CompressionCodec codec = (CompressionCodec) ReflectionUtils
					.newInstance(Class.forName(codecMethod), conf);
			CompressionOutputStream out = codec.createOutputStream(System.out);
			IOUtils.copyBytes(System.in, out, 4096, false);
			out.finish();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void listFile() {
		String path = "hdfs://172.16.5.100:9000/test";
		Configuration conf = new Configuration();
		FileSystem fs;
		try {
			fs = FileSystem.get(URI.create(path), conf);
			FileStatus[] listStatus = fs.listStatus(new Path(path));
			for (FileStatus fileStatus : listStatus) {
				System.out.println(fileStatus.getPath());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void mkdir() {
		String path = "hdfs://192.168.99.156:9000/test/t2";
		Configuration conf = new Configuration();
		FileSystem fs;
		try {
			fs = FileSystem.get(URI.create(path), conf);
			fs.mkdirs(new Path(path));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void test() {
		InputStream in = System.in;
		byte[] bytes = new byte[1024];
		try {
			in.read(bytes);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(new String());
		try {
			in.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void fileWrite1() {

		InputStream in = null;
		FSDataOutputStream out = null;
		try {
			String path = "hdfs://192.168.99.156:9000/test/file1.txt";
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(path), conf);
			out = fs.create(new Path(path));
			in=new FileInputStream(new File("C:/Users/cheng/Desktop/a.txt"));
			IOUtils.copyBytes(in, out, 4096, false);
			System.out.println(out.getPos());
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			IOUtils.closeStream(out);
			IOUtils.closeStream(in);
		}

	}

	private static void fileRead1() {
		InputStream in = null;
		try {
			URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
			in = new URL("hdfs://192.168.99.156:9000/test/file1.txt")
					.openStream();
			IOUtils.copyBytes(in, System.out, 4096, false);
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			IOUtils.closeStream(in);
		}
	}

	private static void fileRead2() {
		FSDataInputStream in = null;
		try {
			String path = "hdfs://192.168.99.156:9000/test/file1.txt";
			Configuration conf = getConfig();
			FileSystem fs = FileSystem.get(URI.create(path), conf);
			in = fs.open(new Path(path));
			IOUtils.copyBytes(in, System.out, 4096, false);
//			System.out.println();
//            in.seek(1);
//            IOUtils.copyBytes(in, System.out, 4096, false);
			//			byte[] bytes = new byte[1024];
//			in.read(0, bytes, 1, 1000);
//			System.out.println(new String(bytes));
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}
	}
}
