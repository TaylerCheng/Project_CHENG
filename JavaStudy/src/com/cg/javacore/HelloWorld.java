package com.cg.javacore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

/**
 * @author： Cheng Guang
 * @date： 2017/6/15.
 */
public class HelloWorld {

    public static void main(String[] args) {
        System.out.println("hello");
        File file = new File("test.txt");
        PrintStream ps = null;
        try {
            ps = new PrintStream(file);
            ps.print("我已经执行啦！");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
