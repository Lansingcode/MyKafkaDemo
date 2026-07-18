package org.bigdata.kafka.kafka;

import com.alibaba.fastjson.JSON;
import com.sun.org.apache.xpath.internal.operations.Or;
import org.bigdata.kafka.entity.Order;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

/**
 *
 */
public class MyProducer {

    private final static String TOPIC_NAME="test1";

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // 设置参数
        Properties props=new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop01.hdp.com:6667");

        // 把发送的key从字符串序列化为字节数组
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // 把发送的value从字符串序列号为字节数组
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // 创建生产消息的客户端，传入参数
        Producer<String,String> producer=new KafkaProducer<String,String>(props);

        int msgNum=5;
        final CountDownLatch countDownLatch=new CountDownLatch(msgNum);

        for (int i=1;i<=10;i++){
            Order order=new Order(Long.valueOf(i),i);

            // key：决定了往哪个分区上发，value：具体要发的消息内容
            ProducerRecord<String,String> producerRecord=new ProducerRecord<>(TOPIC_NAME,
                    order.getOrderId().toString(),JSON.toJSONString(order));

            // 发送消息，得到元数据
            RecordMetadata recordMetadata=producer.send(producerRecord).get();
            System.out.println("异步方式发送消息结果： "+"topic-"+recordMetadata.topic()+"|partition-"+recordMetadata.partition()+"|offset-"+recordMetadata.offset());
        }

    }
}
