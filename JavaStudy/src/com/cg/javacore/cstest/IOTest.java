package com.cg.javacore.cstest;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

public class IOTest {

	public static void main(String[] args) {
		//inTest();
		//outTest();
		download();
	}

	private static void download() {

		File f = new File("C:\\Users\\cheng\\Desktop\\cheng.jpg");		
		FileOutputStream fos = null;
		InputStream fis =null;
		//ByteArrayOutputStream btos=null;
		BufferedOutputStream bos = null;
		try {					
			//从URL中获取输入流
			URL url = new URL("http://map.ps123.net/china/UploadFile/201401/2014011807013848.jpg");
			URLConnection uc = url.openConnection();
			//fis = uc.getInputStream();
			fis=new FileInputStream(new File("C:\\Users\\cheng\\Desktop\\baidu.jpg"));
			
			fos = new FileOutputStream(f);
			bos = new BufferedOutputStream(fos);
			
			int n =0;
			byte[] bts = new byte[1024];
			while ((n = fis.read(bts)) != -1) {
				bos.write(bts,0,n);
			}
//			btos = new ByteArrayOutputStream();   
//			while ((n = fis.read(btos)) != -1) {
//				bos.write(bts,0,n);
//			}
//			fos.write(bos.toByteArray());
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				fis.close();
				fos.close();
				bos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private static void outTest() {

		File f = new File("C:\\Users\\cheng\\Desktop\\test.txt");
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(f);
			int n = 0;
			String str = "cheng\r\nshao";
			byte[] bts =str.getBytes();;
			fos.write(bts);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				fos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		}

	private static void inTest() {
		File f = new File("C:\\Users\\cheng\\Desktop\\test.txt");
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(f);
			int n = 0;
			byte[] bts = new byte[1024];
			// n为实际读取字节数
			while ((n = fis.read(bts)) != -1) {
				System.out.println(n);
				System.out.println(new String(bts, "utf-8"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				fis.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
