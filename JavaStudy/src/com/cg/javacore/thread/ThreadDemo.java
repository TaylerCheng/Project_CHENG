package com.cg.javacore.thread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadDemo implements Runnable {

	private static volatile double d = 1;

	public static void main(String[] args) throws InterruptedException {
//		try {
//			ExecutorService es = Executors.newCachedThreadPool();
//			es.execute(new ThreadDemo());
//			es.execute(new ThreadDemo());
//		} catch (Exception e) {
//			// TODO: handle exception
//		}
//        ExecutorService es = Executors.newCachedThreadPool();
//        es.execute(new ThreadDemo());
//        es.execute(new ThreadDemo());

       Thread thread = new Thread(new ThreadDemo());
        Thread thread2 = new Thread(new ThreadDemo());
        thread.start();
        //测试一个线程异常不会影响其他线程
        thread2.start();
//        Thread.sleep(1000);
        System.out.println("子线程异常不会影响主线程");
    }

	@Override
	public void run() {
		System.out.println("当前线程：" + Thread.currentThread().toString());
		throw new RuntimeException();
	}
}
