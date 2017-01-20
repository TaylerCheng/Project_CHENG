package com.cg.javacore;

import org.junit.Test;

import java.util.ArrayList;
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
        double r = (double)a / b;
        System.out.println(r);
    }

    /**
     * 测试类型比较
     */
    @Test
    public void test002() {
        float f = Float.valueOf("0.7");
        double d = 0.7;
        System.out.println(f==d);
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

}