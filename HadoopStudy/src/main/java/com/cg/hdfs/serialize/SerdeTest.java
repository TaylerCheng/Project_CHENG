package com.cg.hdfs.serialize;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

public class SerdeTest {

	private static Configuration conf = new Configuration();
	
	public static void main(String[] args) throws Exception {
		//序列化到本地
		VIntWritable intValue = new VIntWritable(127);
		serilizeToLocal(intValue);
		
		//从本地恢复到内存
//		IntWritable read = new IntWritable();
//		Path path = new Path("C:/Users/cheng/Desktop/int_out.txt");
//		deserilizeFromLocal(read,path);
		
		//序列化
//		byte[] bytes = serilize(intValue);
//		System.out.println(StringUtils.byteToHexString(bytes));
		//反序列化
//		IntWritable read = new IntWritable();
//		deserilize(read,bytes);
//     	System.out.println(read);
		
	}

	private static byte[] serilize(Writable writable) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(out);
		try {
			writable.write(dos);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				dos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return out.toByteArray();
	}
	
	private static void serilizeToLocal(Writable value) throws FileNotFoundException {
		
		FileOutputStream fos = new FileOutputStream(new File("C:/Users/cheng/Desktop/int_out.txt"));
		DataOutputStream dos = new DataOutputStream(fos);
		try {
			value.write(dos);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				dos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				fos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private static IntWritable deserilize(IntWritable read, byte[] bytes) {

		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		DataInputStream dis = new DataInputStream(in);
		try {
			read.readFields(dis);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				dis.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	private static void deserilizeFromLocal(IntWritable read, Path path) throws IOException {
		FileSystem fs = FileSystem.getLocal(conf);
		FSDataInputStream fis = fs.open(path);
		DataInputStream dis = new DataInputStream(fis);
		try {
			read.readFields(dis);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				dis.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				fis.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}