package com.cg.javacore.thread.consume;

import javax.sound.midi.Soundbank;

/**
 * @author： Cheng Guang
 * @date： 2017/11/30.
 */
public class Consumer implements Runnable {

    private Restaurant restaurant;

    public Consumer(Restaurant restaurant) {
        this.restaurant = restaurant;
    }

    @Override
    public void run() {
        try {
            while (!Thread.interrupted()) {
                synchronized (this) {
                    while (restaurant.meal == null) {
                        System.out.println("消费者释放自身锁");
                        wait();
                    }
                    System.out.println("消费者等待生产者锁");
                    synchronized (restaurant.producer){
                        System.out.println("消费者获得生产者锁");
                        if (restaurant.meal != null){
                            System.out.println("消费完毕 id=" + restaurant.meal.getId());
                            restaurant.meal = null;

                            System.out.println("消费者通知生产者生产");
                            restaurant.producer.notify();
                        }
                    }

                }

            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
