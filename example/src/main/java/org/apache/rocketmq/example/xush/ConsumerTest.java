package org.apache.rocketmq.example.xush;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ConsumerTest {

    public static void main(String[] args) throws Exception {
        String namesrvAddr = "localhost:9876";
        String group = "test_consumer_group";
        String topic = "test_hello_rocketmq";
        // 初始化consumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer();
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.setConsumerGroup(group);
        // 订阅topic
        consumer.subscribe(topic, (String) null);
        // 设置消费的位置，由于producer已经发送了消息，所以我们设置从第一个开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 添加消息监听器
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                msgs.forEach(msg -> {
                    System.out.println(new String(msg.getBody()));
                });
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        // 启动consumer
        consumer.start();
        // 由于是异步消费，所以不能立即关闭，防止消息还未消费到
        TimeUnit.SECONDS.sleep(2);
        consumer.shutdown();
    }
}