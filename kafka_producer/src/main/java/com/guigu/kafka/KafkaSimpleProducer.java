package com.guigu.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaSimpleProducer {
    private final  static String TOPIC_NAME = "my-kafka-topic";
    public static void main( String[] args ) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.11.134:9092");

        //把发送的key从字符串序列化为字节数组
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //创建生产消息的客户端
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //创建消息
        //三个参数 1.消息主题 2.消息的key（未指明发送分区时，通过key底层hash运算决定发送的分区） 3.具体要发送的消息内容
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, "my-fisrt-key", "hello-kafka");

        //发送消息，得到消息发送的元数据并输出
        RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
        System.out.println("同步方式发送消息结果topic为: "+recordMetadata.topic());
        System.out.println("同步方式发送消息结果partition为: "+recordMetadata.partition());
        System.out.println("同步方式发送消息结果offset为: "+recordMetadata.offset());
    }
}
