package com.guigu.kafka;



import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class KafkaSimpleConsumer {
    private final  static String TOPIC_NAME = "my-kafka-topic";
    private final  static  String CONSUMER_NAME = "testGroup";
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.11.134:9092");

        //配置1   消费分组名
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,CONSUMER_NAME);

        //配置2   把消费的key从字节数组反序列化为字符串
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        //配置3  配置手动提交还是自动提交 true为自动提交，false为手动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        //properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");//自动提交offset的间隔时间


        //配置4  一次poll最大拉取消息的条数，可以根据消费速度的快慢来设置
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,500);
        //配置5  如果两次poll的时间如果超出30s的时间间隔，kafka会认为消费能力弱，将其剔除消费者组，出发rabalance机制，把分区分配给其他消费者
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,30*1000);

        //配置6 consumer向broker发送心跳的时间
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,1000);
        //kafka如果超过10s没有收到消费者的心跳，就会把消费者剔除消费者组 进行rabalace，把分区分配给其他消费者
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,10*1000);

        //配置7  新消费者组中的消费者启动后，默认会从当前分区最后一条消息offset+1处开始消费，可以通过设置让新的消费者
         //       第一次从头消费，之后从最后消费位置的offset+1处消费
        // earliest 第一次从头开始消费，之后开始消费新消息
        //latest 默认消费新消息
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");



        //创建一个消费者客户端
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        //消费模式1  消费者者订阅主题未指定分区消费
        //kafkaConsumer.subscribe(Arrays.asList(TOPIC_NAME));
        //消费模式2  消费者订阅主题指定分区消费
        //kafkaConsumer.assign(Arrays.asList(new TopicPartition(TOPIC_NAME,0)));
        //消费模式3  指定主题分区从头消费
        //kafkaConsumer.assign(Arrays.asList(new TopicPartition(TOPIC_NAME,0)));
        //kafkaConsumer.seekToBeginning(Arrays.asList(new TopicPartition(TOPIC_NAME,0));
        //消费模式4 指定主题分区特定偏移量1处开始消费
        kafkaConsumer.assign(Arrays.asList(new TopicPartition(TOPIC_NAME,0)));
        kafkaConsumer.seek(new TopicPartition(TOPIC_NAME,0),1);


        while(true){
           /* poll是拉取消费的长轮询*/
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
            if(consumerRecords!=null && consumerRecords.count()>0){
                for ( ConsumerRecord record : consumerRecords) {
                    System.out.println("收到消息partition： "+record.partition());
                    System.out.println("收到消息key为："+record.key());
                    System.out.println("收到消息value为："+record.value());
                    System.out.println("收到消息offset为："+record.offset());
                }
            }
           // kafkaConsumer.commitSync();//手动同步提交  当前线程会阻塞到offset提交成功

            kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                    if(e!=null){
                        System.out.println("手动异步提交失败");
                    }
                }
            });
        }

    }
}
