package com.cg.javacore.thread;

import java.util.ArrayList;
import java.util.Vector;

public class Dog implements Runnable {

	private static ArrayList<String> names = new ArrayList<String>();

	private String name = null;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public static void main(String[] args) {
		Dog dog = new Dog();

		Thread thread = new Thread(dog);
		thread.start();

		Thread thread2 = new Thread(dog);
		thread2.start();

		for (String name : names) {
			System.out.println(name);
		}
	}

	@Override
	public void run() {
		sayHello();
	};

	public void sayHello() {
		names.add(Thread.currentThread().toString());
	}
}
