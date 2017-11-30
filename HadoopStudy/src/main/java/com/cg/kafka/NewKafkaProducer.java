package com.cg.kafka;

/**
 * @author： Cheng Guang
 * @date： 2017/3/31.
 */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class NewKafkaProducer {

    private final KafkaProducer<String, String> producer;
    public final static String TOPIC = "testTopic";

    private NewKafkaProducer() {
        Properties props = new Properties();
        // 此处配置的是kafka的broker地址:端口列表
        props.put("bootstrap.servers", "master.hadoop:9092,slave1.hadoop:9092,slave2.hadoop:9092");
        //配置zookeeper
        props.put("zookeeper.connect", "master.hadoop:2181,slave1.hadoop:2181,slave2.hadoop:2181");
        //        props.put("zk.connectiontimeout.ms", config.getString("zk.connectiontimeout.ms"));
        //配置value的序列化类
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //配置key的序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("acks", "-1");

        producer = new KafkaProducer<String, String>(props);
    }

    private void produce() {
        int messageNo = 1;
        final int COUNT = 10;

        int messageCount = 0;
        while (messageNo <= COUNT) {
            String key = String.valueOf(messageNo);
            String data = "Hello kafka message :" + key;
            producer.send(new ProducerRecord<String, String>(TOPIC, key, data));
            System.out.println(data);
            messageNo++;
            messageCount++;
        }

        System.out.println("Producer端一共产生了" + messageCount + "条消息！");
    }

    private void produceOneMsg(String message) {

        for (int i = 0; i < 12; i++) {
            String key = String.valueOf(i);
            producer.send(new ProducerRecord<String, String>(TOPIC, key, message + i));
            System.out.println("Producer端发送消息: " + message + i);
        }
//        producer.flush();

    }

    public static void main(String[] args) {
        new NewKafkaProducer().produceOneMsg("I am CG");
    }
}