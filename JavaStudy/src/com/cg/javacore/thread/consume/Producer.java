package com.cg.javacore.thread.consume;

import java.util.Random;

/**
 * @author： Cheng Guang
 * @date： 2017/11/30.
 */
public class Producer implements Runnable {

    private Restaurant restaurant;

    public Producer(Restaurant restaurant) {
        this.restaurant = restaurant;
    }

    @Override
    public void run() {
        try {
            while (!Thread.interrupted()) {
                synchronized (this) {
                    while (restaurant.meal != null) {
                        Thread.sleep(1 * 1000);
                        System.out.println("生产者释放自身锁");
                        wait();
                    }
                    synchronized (restaurant.consumer) {
                        System.out.println("生产者获得消费者锁");
                        if (restaurant.meal == null) {
                            int mealId = new Random().nextInt();
                            System.out.println("生产完毕 id=" + mealId);
                            restaurant.meal = new Meal(mealId);

                            System.out.println("生产者通知消费者消费");
                            restaurant.consumer.notify();
                            System.out.println("notify后我要执行完。。。。。。。");
                        }
                    }
                }

            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
