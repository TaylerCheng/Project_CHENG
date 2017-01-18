package com.cg.javacore.thread;

import java.util.concurrent.TimeUnit;

/**
 * Created by Cheng Guang on 2016/12/19.
 */
public class InterruptTest extends Thread {

    private static String str = "hello";

    private static Object obj = new Object();

    @Override
    public void run() {
        try {
//            while (!isInterrupted()){
//                sleep(100);
//                System.out.println("阻塞了一下");
//            }
            synchronized (str){
                System.out.println("t1线程获得str锁");
                System.out.println("t1线程等待obj锁");
                synchronized (obj){
                    System.out.println("t1线程获得obj锁");
                    obj.wait();
                }
            }
            System.out.println("没有抛出异常");
        } catch (InterruptedException e) {
            System.out.println("抛出异常, isInterrupted:" + isInterrupted());
        }
    }

    public static void main(String[] args) throws InterruptedException {
        InterruptTest t1 = new InterruptTest();
        t1.start();
//        TimeUnit.SECONDS.sleep(1);
//        t1.interrupt();
        synchronized(obj) {
            System.out.println("主线程获得obj锁");
            TimeUnit.SECONDS.sleep(1);
            System.out.println("主线程打断t1");
            t1.interrupt();
            TimeUnit.SECONDS.sleep(5);
            System.out.println("主线程释放obj锁");
        }

    }

}
