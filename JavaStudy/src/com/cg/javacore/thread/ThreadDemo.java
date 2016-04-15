package com.cg.javacore.thread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadDemo implements Runnable {

	private static volatile double d = 1;

	public static void main(String[] args)  {
		try {
			ExecutorService es = Executors.newCachedThreadPool();
			es.execute(new ThreadDemo());
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	@Override
	public void run() {
		throw new RuntimeException();
	}
}
