package com.cg.javacore.reflect;

public class HelloWorld {
    public String str = null;
    private int id = 0;

    public HelloWorld() {

    }

    public HelloWorld(String str) {
        this.str = str;
    }

    public void sayHello() {
        System.out.println("Hello");
    }

    public void sayYes() {
        System.out.println("Yes");
    }

    public static void main(String[] args) {
        long start_time = System.currentTimeMillis();
        for (int i = 0; i < 100 * 10000; i++) {
            boolean b = 16 == (16 & 16);
        }
        long end_time = System.currentTimeMillis();
        System.out.println("运用与运算比较用时：" + (end_time - start_time));

        start_time = end_time;
        for (int i = 0; i < 100 * 10000; i++) {
            boolean b = 16 == 16;
        }
        end_time = System.currentTimeMillis();
        System.out.println("直接比较用时：" + (end_time - start_time));
    }
}
