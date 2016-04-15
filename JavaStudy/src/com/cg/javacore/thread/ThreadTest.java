package com.cg.javacore.thread;

import com.cg.javacore.reflect.Test;

public class ThreadTest extends Thread{

	public static void main(String[] args) {
		Test test1 = new Test();
		Test test2 = new Test();

		TicketWindow tw = new TicketWindow(test1, test2);
		Thread t1 = new Thread(tw);
		// Thread t2 = new Thread(tw);
		// Thread t3 = new Thread(tw);
		t1.start();
		// t2.start();
		// t3.start();

		TicketCenter tc = new TicketCenter(test1, test2);
		Thread t4 = new Thread(tc);
		t4.start();
	}

}

class TicketWindow implements Runnable {

	private int ticketNumber = 10;
	Test test1, test2;

	public TicketWindow(Test test1, Test test2) {
		this.test1 = test1;
		this.test2 = test2;
	}

	@Override
	public void run() {

//		while (true) {
//			 synchronized (obj1) {
//			
//			 if (ticketNumber > 0) {
//			 System.out.print(Thread.currentThread().getName()+"正在占用      ");
//			 System.out.println(Thread.currentThread().getName()
//			 + "正在售第" + ticketNumber + "张票");
//			 try {
//			 Thread.sleep(1000);
//			 } catch (InterruptedException e) {
//			 e.printStackTrace();
//			 }
//			 ticketNumber--;
//			 } else {
//			 break;
//			 }
//			 }
//		}
		synchronized (test1) {
			System.out.println(Thread.currentThread().getName()+"占用test1");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println(Thread.currentThread().getName()+"正在等待test2");
			synchronized (test2) {
				
			}
		}
	}
}

class TicketCenter implements Runnable {

	private int ticketNumber = 100;
	Test test1, test2;

	public TicketCenter(Test test1, Test test2) {
		this.test1 = test1;
		this.test2 = test2;
	}

	@Override
	public void run() {
		synchronized (test2) {
			System.out.println(Thread.currentThread().getName()+"占用test2");
			System.out.println(Thread.currentThread().getName()+"正在等待test1");
			synchronized (test1) {

			}
		}
	}
}