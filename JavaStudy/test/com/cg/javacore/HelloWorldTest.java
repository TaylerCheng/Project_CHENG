package com.cg.javacore;

import com.mongodb.internal.HexUtils;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.junit.Test;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author： Cheng Guang
 * @date： 2016/12/24.
 */
public class HelloWorldTest {

    /**
     * 测试类型转换问题
     */
    @Test
    public void test001() {
        int a = 2;
        int b = 6;
        double r = (double) a / b;
        System.out.println(r);
    }

    /**
     * 测试类型比较
     */
    @Test
    public void test002() {
        float f = Float.valueOf("0.7");
        double d = 0.7;
        System.out.println(f == d);
    }

    /**
     * 测试异常抛出
     */
    @Test
    public void test003() throws Exception {
        List<String> list = new ArrayList<>();
        try {
            list.add("a");
            throw new RuntimeException();
        } catch (Exception e) {
            list.add("qing");
            throw new Exception(e);
        } finally {
            System.out.println(list);
        }
    }

    @Test
    public void test004() throws Exception {
        Long nullLong = null;
        long l = nullLong;
        System.out.println(l);
    }

    @Test
    public void test005() throws Exception {
        Date date = new Date();
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        System.out.println(calendar.getTime());
    }

    @Test
    public void test006() throws Exception {
        List<String> list = new ArrayList<String>();
        list.add("1");
        list.add("2");
        //        list.add("3");
        //        list.add("4");
        for (String temp : list) {
            if ("1".equals(temp)) {
                list.remove(temp);
            }
            System.out.println(temp);
        }
        System.out.println(list);
    }

    @Test
    public void test007() throws Exception {
        String str = null;
        if (str == null || str.contains("")) {
            System.out.println(str);
        }
    }

    @Test
    public void test008() throws Exception {
        String str = "程";
        System.out.println(HexUtils.toHex(str.getBytes("utf-8")));
        System.out.println(str.startsWith(""));
    }

    @Test
    public void test009() throws Exception {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_MONTH, -90);
        System.out.println(calendar.getTime());
    }

    @Test
    public void test010() throws Exception {
        System.out.println(System.getProperties().getProperty("file.encoding"));
        System.out.println(Charset.defaultCharset());
    }

    @Test
    public void test011() throws Exception {
        FileInputStream fis = new FileInputStream("C:\\Users\\Administrator\\Desktop\\UTF-8+.txt");

        BOMInputStream bomIn = new BOMInputStream(fis, ByteOrderMark.UTF_8);

        BufferedReader br = new BufferedReader(new InputStreamReader(fis));

        String line = br.readLine();
        System.out.println(line);
        br.close();
    }

}