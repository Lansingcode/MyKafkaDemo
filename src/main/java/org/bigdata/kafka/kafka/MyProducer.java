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

        // 配置ack参数
        props.put(ProducerConfig.ACKS_CONFIG,"1");

        //失败重试次数
        props.put(ProducerConfig.RETRIES_CONFIG,3);

        //失败重试间隔
        props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG,300);

        // 创建生产消息的客户端，传入参数
        Producer<String,String> producer=new KafkaProducer<String,String>(props);

        int msgNum=5;
        final CountDownLatch countDownLatch=new CountDownLatch(msgNum);

        for (int i=1;i<=5;i++){
            Order order=new Order(Long.valueOf(i),i);

            // 指定发送分区
//            ProducerRecord<String,String> producerRecord=new ProducerRecord<>(TOPIC_NAME,0,
//                    order.getOrderId().toString(),JSON.toJSONString(order));
            // 未指定分区
            // key：决定了往哪个分区上发，value：具体要发的消息内容
            ProducerRecord<String,String> producerRecord=new ProducerRecord<>(TOPIC_NAME,
                    order.getOrderId().toString(),JSON.toJSONString(order));

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
//            producer.send(producerRecord,new Callback(){
//                public void onCompletion(RecordMetadata recordMetadata,Exception exception){
//                    if(exception!=null){
//                        System.err.println("消息发送失败："+exception.getStackTrace());
//                    }
//                    if (recordMetadata!=null){
//                        System.out.println("异步方式发送消息结果： "+"topic-"+recordMetadata.topic()+"|partition-"+recordMetadata.partition()+"|offset-"+recordMetadata.offset());
//
//                    }
//                    countDownLatch.countDown();
//                }
//            });

        }

        countDownLatch.await(5, TimeUnit.SECONDS);
        producer.close();

    }
}
