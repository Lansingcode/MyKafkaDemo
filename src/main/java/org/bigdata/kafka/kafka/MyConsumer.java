package org.bigdata.kafka.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class MyConsumer {
    private final static String TOPIC_NAME="test1";
    private final static String CONSUMER_GROUP_NAME="testGroup";

    public static void main(String[] args) {
        Properties props=new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop01.hdp.com:6667");

        // 配置消费组
        props.put(ConsumerConfig.GROUP_ID_CONFIG,CONSUMER_GROUP_NAME);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        // 创建消费者客户端
        KafkaConsumer<String,String> consumer= new KafkaConsumer<String,String>(props);

        // 订阅主题列表
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        while (true){
            ConsumerRecords<String,String> records=consumer.poll(1000L);

            for (ConsumerRecord<String,String> record:records){
                System.out.printf("收到消息：partition=%d,offset=%d,key=%s,value=%s%n",record.partition(),
                        record.offset(),record.key(),record.value());
            }
        }

    }
}
