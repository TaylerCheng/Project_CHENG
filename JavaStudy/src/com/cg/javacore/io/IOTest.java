package com.cg.javacore.io;

import com.mongodb.internal.HexUtils;
import org.junit.Test;

import java.io.*;
import java.net.URL;

/**
 * Created by Cheng Guang on 2016/9/23.
 */
public class IOTest {

    public static void main(String[] args) {
        InputStream inputStream = null;

        try {
            inputStream = new BufferedInputStream(IOTest.class.getResourceAsStream("/db.properties"));
            byte[] bytes = new byte[10];

            int readNum;
//            while ((readNum = inputStream.read(bytes)) != -1) {
//                System.out.print(new String(bytes));
//            }

            int count=0;
            while ((readNum = inputStream.read()) != -1) {
                System.out.println((byte)readNum);
                count++;
            }
            System.out.println(count);


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    @Test
    public void streamingTest(){
        InputStream in = null;
        OutputStream out = null;

        Reader reader = null;
        Writer writer = null;
        try {
            in = new FileInputStream("C:\\Users\\Administrator\\Desktop\\a.txt");
            byte[] bytes = new byte[1024];
            int len;
            while ((len = in.read(bytes)) != -1) {
                System.out.println(HexUtils.toHex(bytes));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void readerTest(){
        Reader reader = null;
        Writer writer = null;
        try {
            reader = new InputStreamReader(new FileInputStream("C:\\Users\\Administrator\\Desktop\\a.txt"),"UTF-8");
            char[] data = new char[10];
            reader.read(data);
            for (char c : data) {
                System.out.println(c);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
