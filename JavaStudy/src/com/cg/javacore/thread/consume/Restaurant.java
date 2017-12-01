package com.cg.javacore.thread.consume;


/**
 * @author： Cheng Guang
 * @date： 2017/11/30.
 */
public class Restaurant {

    public Meal meal = null;
    public Producer producer;
    public Consumer consumer;

    public Restaurant() {
        producer = new Producer(this);
        consumer = new Consumer(this);
    }

    public static void main(String[] args) throws InterruptedException {
        Restaurant restaurant = new Restaurant();

        Thread thread1 = new Thread(restaurant.producer);
        thread1.start();

        Thread thread2 = new Thread(restaurant.consumer);
        thread2.start();

        Thread.currentThread().sleep(10*1000);
        thread1.interrupt();
        thread2.interrupt();
    }

}

