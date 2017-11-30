package com.cg.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author： Cheng Guang
 * @date： 2017/3/31.
 */

public class ChubaoConsumer {

    public static final String CHUBAO_TOPIC = "chubao";

    private final ConsumerConnector consumer;

    private ChubaoConsumer() {
        Properties props = new Properties();
        // zookeeper 配置
//        props.put("zookeeper.connect", "slave1.hadoop:2181");
        props.put("zookeeper.connect", "192.168.100.235:2181,192.168.100.236:2181,192.168.100.234:2181");
        // 消费者所在组
        props.put("group.id", "testgroup2");
//        props.put("producer.type", "sync");
        // 序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        // zk连接超时
//        props.put("zookeeper.session.timeout.ms", "4000");
//        props.put("zookeeper.sync.time.ms", "200");
//        props.put("auto.commit.interval.ms", "1000");
//        props.put("auto.offset.reset", "smallest");

        ConsumerConfig config = new ConsumerConfig(props);
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
    }

    public void consume() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(CHUBAO_TOPIC, new Integer(1));

        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        Map<String, List<KafkaStream<String, String>>> consumerMap =
                consumer.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);
        KafkaStream<String, String> stream = consumerMap.get(CHUBAO_TOPIC).get(0);
        ConsumerIterator<String, String> it = stream.iterator();

        int messageCount = 0;
        while (it.hasNext()){
            System.out.println(it.next().message());
            messageCount++;
        }
        System.out.println("Consumer端一共消费了" + messageCount + "条消息！");
    }

    public static void main(String[] args) {
        new ChubaoConsumer().consume();
    }
}