教程内容

根据B站视频教程  
[Kafka从入门到实战全套教程（31集全）](https://www.bilibili.com/video/BV1sP2RYQEj8?vd_source=69dd5217d16adc5656c317f744b761e3&spm_id_from=333.788.player.switch&p=14)
编写代码  
运行前先启动Ambari，代码中地址都是Ambari配置的，如果用在其他地方需要根据情况修改。  


## 相关路径
```shell
// 安装路径
cd /usr/hdp/3.1.0.0-78/kafka

//配置路径
/usr/hdp/3.1.0.0-78/kafka/conf
vim server.properties

//日志路径
cd /kafka-logs
```
启动kafka
```shell
./kafka-server-start.sh -daemon ../config/server.properties
```
查看是否启动
```shell
ps -aux | grep server.properties
```
创建topic
```shell
./kafka-topics.sh --create --zookeeper hadoop03.hdp.com:2181,hadoop02.hdp.com:2181,hadoop01.hdp.com:2181/kafka --replication-factor 1 --partitions 1 --topic test
```
查看已创建topic
```shell
./kafka-topics.sh --list --zookeeper hadoop03.hdp.com:2181,hadoop02.hdp.com:2181,hadoop01.hdp.com:2181/kafka
```

在zookeeper中查看broker id 和topic
```shell
// 进入zookeeper安装目录
cd /usr/hdp/3.1.0.0-78/zookeeper/bin

// 进入客户端
./zkCli.sh -server hadoop01.hdp.com:2181/kafka

// 查看broker id
ls /brokers/ids
// 查看topic
ls /brokers/topics
```
把消息发送给topic
```shell
./kafka-console-producer.sh --broker-list hadoop01.hdp.com:6667 --topic test
```
消费kafka中的数据
```shell
// 消费数据
./kafka-console-consumer.sh --bootstrap-server hadoop01.hdp.com:6667 --topic test

// 从头消费
./kafka-console-consumer.sh --bootstrap-server hadoop01.hdp.com:6667 --from-beginning --topic test
```
单播消息
在一个kafka的topic中，启动两个消费者，一个生产者，
如果多个消费者在同一个消费者组，那么只有一个消费者可以收到订阅的topic中的消息。
```shell
./kafka-console-consumer.sh --bootstrap-server hadoop01.hdp.com:6667 --consumer-property group.id=testGroup --topic test
```

多播消息
不同的消费组订阅同一个topic，每个消费组中只有一个消费者能收到消息。
```shell
// 消费者1
cd /usr/hdp/3.1.0.0-78/kafka/bin
./kafka-console-consumer.sh --bootstrap-server hadoop01.hdp.com:6667 --consumer-property group.id=testGroup --topic test

// 消费者2
cd /usr/hdp/3.1.0.0-78/kafka/bin
./kafka-console-consumer.sh --bootstrap-server hadoop01.hdp.com:6667 --consumer-property group.id=testGroup1 --topic test
```

查看消费组及信息
查看消费者组
```shell
cd /usr/hdp/3.1.0.0-78/kafka/bin
./kafka-consumer-groups.sh --bootstrap-server hadoop01.hdp.com:6667 --list
```
查看消费者组详细信息
```shell
cd /usr/hdp/3.1.0.0-78/kafka/bin
./kafka-consumer-groups.sh --bootstrap-server hadoop01.hdp.com:6667 --describe --group testGroup
```
重点关注以下信息
current-offset:最后被消费的消息的偏移量
Log-end-offset:消息总量
Lag：积压了多少消息

主题和分区
创建多分区topic
```shell
cd /usr/hdp/3.1.0.0-78/kafka/bin

./kafka-topics.sh --create --zookeeper hadoop03.hdp.com:2181,hadoop02.hdp.com:2181,hadoop01.hdp.com:2181/kafka --replication-factor 1 --partitions 2 --topic test1
```
查看日志
```shell
cd /kafka-logs/
```
两个分区对应两个文件夹
<!-- 这是一张图片，ocr 内容为： -->
![](https://cdn.nlark.com/yuque/0/2026/png/231911/1784337654394-038f18bd-fb4e-4c3d-946e-360c9edd1ba5.png)

## 稀疏索引

消息日志文件中保存的内容
00000000.log：保存消息内容
__consumer_offsets-0~__consumer_offsets-49：kafka内部创建了__consumer_offsets主题包含50个分区，这个主题用于存放消费者消费某个topic的偏移量，key是consumerGroupId+topic+分区号，value是当前offset值，kafka会定期清理topic内的消息。因为__consumer_offsets可能会接收高并发的请求，kafka默认给其分配50个分区（可通过offsets.topic.num.partitions设置），这样可以通过加机器的方式抗大并发。通过如下公式选出consumer消费的offset要提交到__consumer_offsets哪个分区
公式：hash(consumerGroupId)%__consumer_offsets主题的分区数
文件中保存的消息，默认保存7天，过期删除


## ACK配置
ACK有三个配置  
ack=0  
ack=1  
ack=-1  
如果没有收到ack就开启重试

## 关于消息发送的缓冲区  
- kafka默认会创建一个消息缓冲区，用来存放要发送的消息，缓冲区大小为32M  
- kafka本地线程从缓冲区中一次拉取16k的数据，发送到broker  
- 如果线程拉不到16k的数据，间隔10ms也会将已拉取的数据发送到broker  

## 消费者的实现细节  
