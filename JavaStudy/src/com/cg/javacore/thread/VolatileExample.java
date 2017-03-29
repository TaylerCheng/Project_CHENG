package com.cg.javacore.thread;

/**
 * @author： Cheng Guang
 * @date： 2017/3/24.
 */
public class VolatileExample extends Thread{
    boolean isWriter;

    private volatile static int a = 0;
    private volatile static boolean flag = true;

    public VolatileExample(boolean isWriter) {
        this.isWriter = isWriter;
    }

    public void writer() {
        a = 1;                   //1
        flag = true;               //2
    }

    public void reader() {
        if (flag) {                //3
//            int i = a;
            if (a == 0) {
                System.out.println("haha");
            }
        }
    }

    @Override
    public void run() {
        if (isWriter) {
            while (true)
                writer();
        } else {
            while (true)
                reader();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        new VolatileExample(true).start();
        new VolatileExample(false).start();
    }

}
