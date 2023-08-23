package org.apache.rocketmq.example.xush;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

public class ProducerTest {
    public static void main(String[] args) throws Exception {
        String namesrvAddr = "localhost:9876";
        String group = "test_group";
        String topic = "test_hello_rocketmq";
        // 构建Producer实例
        DefaultMQProducer producer = new DefaultMQProducer();
        // <1.2> 设置 RocketMQ Namesrv 地址
        producer.setNamesrvAddr(namesrvAddr);
        producer.setProducerGroup(group);
        // 启动producer
        producer.start();
        // 发送消息
        SendResult result = producer.send(new Message(topic, "one".getBytes()));
        System.out.println(result.getSendStatus());
        // 关闭producer
        producer.shutdown();
    }
}