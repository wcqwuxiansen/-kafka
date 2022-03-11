package com.guigu.kafka;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaSimpleProducer {
    private final  static String TOPIC_NAME = "my-kafka-topic";
    public static void main( String[] args ) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.11.134:9092");

        //配置1   把发送的key从字符串序列化为字节数组
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //配置2   消息持久化机制参数
       /* acks=0 表示生产者不需要等待任何broker确认收到消息的回复，就可以继续发送下一条消息。性能最高，但是最容易丢失消息
       *  acks=1 表示至少要等待leader已经成功的将数据写入到本地的log，但不需要follower是否成功写入，就可以继续发送下一条消息
       *    ，这种情况下，如果follower没有成功的备份数据，而此时leader又挂掉，容易丢失消息
       *  ack=-1/all 需要等待min.insync.replicas(默认为1，推荐配置为2)  这个参数配置的副本个数都成功写入了日志，这种策略会保证
       *        只要至少有一个备份存活就不会丢失数据。
       *
       * */
        properties.put(ProducerConfig.ACKS_CONFIG,"1");


        //配置3   重试机制参数
        //发送失败会重试，默认重试间隔个100ms，重试能保证消息发送的可靠性，但也有可能造成消息重复发送，比如网络抖动，所以要在接收者端做好幂等性
        properties.put(ProducerConfig.RETRIES_CONFIG,3); //重试次数
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,300);//重试间隔时间

        //配置4   生产者消息发送缓冲区设置
        //设置发送消息的本地缓冲区，如果设置了该缓冲区，消息会先发送到缓冲区，可以提高发送性能 默认值33554432 即32mb
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384); //本地线程会从缓冲区取数据，批量发送到broker，批量发送的大小就是16384 字节，即16kb
        properties.put(ProducerConfig.LINGER_MS_CONFIG,10);//默认值为0 即立即发送数据 一般设置为10ms 如果在10ms内能够取满16kb直接发送
                                                            // 10ms内数据不够16kb 10ms后直接发送消息 不能让消息发送延迟时间太长



        //创建生产消息的客户端
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //创建消息
        //三个参数 1.消息主题 2.消息的key（未指明发送分区时，通过key底层hash运算决定发送的分区） 3.具体要发送的消息内容
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, "my-fisrt-key", "hello-kafka");

        //同步发送消息方式，得到消息发送的元数据并输出
       /* RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
        System.out.println("同步方式发送消息结果topic为: "+recordMetadata.topic());
        System.out.println("同步方式发送消息结果partition为: "+recordMetadata.partition());
        System.out.println("同步方式发送消息结果offset为: "+recordMetadata.offset());*/

       //异步方式发送消息
       kafkaProducer.send(producerRecord, new Callback() {
           @Override
           public void onCompletion(RecordMetadata recordMetadata, Exception e) {
               if(e !=null){
                   System.out.println("消息发送失败"+e.getMessage());
               }

               if(recordMetadata != null){
                   System.out.println("同步方式发送消息结果topic为: "+recordMetadata.topic());
                   System.out.println("同步方式发送消息结果partition为: "+recordMetadata.partition());
                   System.out.println("同步方式发送消息结果offset为: "+recordMetadata.offset());
               }
           }
       });

       while(true){

       }
    }
}
