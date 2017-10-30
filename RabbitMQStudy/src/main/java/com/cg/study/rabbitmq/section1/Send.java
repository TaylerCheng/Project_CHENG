package com.cg.study.rabbitmq.section1;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.concurrent.TimeoutException;

/**
 * @author： Cheng Guang
 * @date： 2017/10/25.
 */
public class Send {

    private final static String QUEUE_NAME = "hello";


    public static void main(String[] argv)
            throws java.io.IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        for (int i = 0; i < 40; i++) {
            if (i==20){
                Thread.sleep(5000);
            }else if (i>20){
                Thread.sleep(1000);
            }
            String message = "Hello,I am " + i;
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");
        }

        channel.close();
        connection.close();
    }

}
