package org.bigdata.kafka.kafka;

import com.alibaba.fastjson.JSON;
import org.bigdata.kafka.entity.Order;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MySimpleProducer {

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


        // key：决定了往哪个分区上发，value：具体要发的消息内容
        ProducerRecord<String,String> producerRecord=new ProducerRecord<>(TOPIC_NAME,
                "mykey","myvalue");

        //同步发送消息
        try{
            RecordMetadata recordMetadata=producer.send(producerRecord).get();
            // 如果没有收到ack会阻塞
            System.out.println("同步方式发送消息结果："+"topic-"+recordMetadata.topic()+"|partition-"+
                    recordMetadata.partition()+"|offset-"+recordMetadata.offset());
        }catch (InterruptedException e){
            e.printStackTrace();

            Thread.sleep(1000);
            try {
                RecordMetadata recordMetadata=producer.send(producerRecord).get();
            } catch (Exception e1){

            }
        } catch (ExecutionException e){
            e.printStackTrace();
        }


        // 异步发送消息
//        producer.send(producerRecord,new Callback(){
//            public void onCompletion(RecordMetadata recordMetadata,Exception exception){
//                if(exception!=null){
//                    System.err.println("消息发送失败："+exception.getStackTrace());
//                }
//                if (recordMetadata!=null){
//                    System.out.println("异步方式发送消息结果： "+"topic-"+recordMetadata.topic()+"|partition-"+recordMetadata.partition()+"|offset-"+recordMetadata.offset());
//
//                }
//                countDownLatch.countDown();
//            }
//        });
//        Thread.sleep(10000000L);
    }
}
