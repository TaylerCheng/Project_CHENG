package com.cg.javacore.thread;

public class Dog implements Runnable {

	private String name = "oldName";

	@Override
	public void run() {
		try {
			changeName();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void changeName() throws InterruptedException {
		synchronized (this) {
			System.out.println(Thread.currentThread().getName()+"获得对象锁");
			System.out.println(Thread.currentThread().getName()+"狗的名字"+this.name);
			Thread.currentThread().sleep(10 * 1000);
			System.out.println(Thread.currentThread().getName()+"狗的名字"+this.name);
			System.out.println(Thread.currentThread().getName()+"释放对象锁");
		}
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
