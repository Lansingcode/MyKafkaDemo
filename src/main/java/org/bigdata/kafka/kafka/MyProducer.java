package org.bigdata.kafka.kafka;

import com.alibaba.fastjson.JSON;
import com.sun.org.apache.xpath.internal.operations.Or;
import org.bigdata.kafka.entity.Order;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class MyProducer {

    private final static String TOPIC_NAME="test1";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props=new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop01.hdp.com:6667");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        Producer<String,String> producer=new KafkaProducer<String,String>(props);

        int msgNum=5;
        final CountDownLatch countDownLatch=new CountDownLatch(msgNum);

        for (int i=1;i<=10;i++){
            Order order=new Order(Long.valueOf(i),i);

            ProducerRecord<String,String> producerRecord=new ProducerRecord<>(TOPIC_NAME,
                    order.getOrderId().toString(),JSON.toJSONString(order));

            producer.send(producerRecord).get();
        }

    }
}
