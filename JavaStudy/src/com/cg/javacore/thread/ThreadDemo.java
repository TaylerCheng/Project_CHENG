package com.cg.javacore.thread;

import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

public class ThreadDemo implements Runnable {

    private static int count = 0;

    private final int id = count++;

    public static void main(String[] args) throws InterruptedException {
        try {
            //			ExecutorService es = Executors.newCachedThreadPool();
            //			es.execute(new ThreadDemo());
            //			es.execute(new ThreadDemo());
        } catch (Exception e) {
        }

        ExecutorService es = new ThreadPoolExecutor(2, 4, 10*000, TimeUnit.MINUTES, new LinkedBlockingQueue(2),
                new ThreadPoolExecutor.DiscardOldestPolicy());
        es.execute(new ThreadDemo());
//        es.execute(new ThreadDemo());
//        es.execute(new ThreadDemo());
//        es.execute(new ThreadDemo());
//        es.execute(new ThreadDemo());
//        es.execute(new ThreadDemo());
//        es.execute(new ThreadDemo());
//        es.execute(new ThreadDemo());
//        es.execute(new ThreadDemo());
//        es.execute(new ThreadDemo());

        List<Runnable> runnables = es.shutdownNow();
        System.out.println(runnables.size());

        //       Thread thread = new Thread(new ThreadDemo());
        //        Thread thread2 = new Thread(new ThreadDemo());
        //        thread.start();
        //        //测试一个线程异常不会影响其他线程
        //        thread2.start();
        ////        Thread.sleep(1000);
        //        System.out.println("子线程异常不会影响主线程");
    }

    @Override
    public void run() {
        System.out.println("当前线程：" + Thread.currentThread().toString() + "  id = " + id);
        try {
            TimeUnit.SECONDS.sleep(5);
            while (1==1){
                for (int i = 0; i <100000 ; i++) {
                    double v = Math.PI * Math.PI* new Random().nextDouble();
                }
                System.out.print(".");
            }
        } catch (InterruptedException e) {
            System.out.println("执行被打断 id = " + id);
        }
//            System.out.println("我是流氓软件，我还可以执行");

    }
}
