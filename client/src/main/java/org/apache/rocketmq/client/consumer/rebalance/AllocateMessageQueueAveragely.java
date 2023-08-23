/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Average Hashing queue algorithm
 * <p>
 * 平均哈希队列算法
 */
public class AllocateMessageQueueAveragely extends AbstractAllocateMessageQueueStrategy {

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
                                       List<String> cidAll) {

        List<MessageQueue> result = new ArrayList<>();
        if (!check(consumerGroup, currentCID, mqAll, cidAll)) {
            return result;
        }

        // 当前客户端是否为当前topic的消费者
        int index = cidAll.indexOf(currentCID);
        // 取余
        int mod = mqAll.size() % cidAll.size();
        /*
         * averageSize: currentCID可以分配到的队列数
         * 如果队列大于消费者
         *     averageSize = mod > 0 && index < mod ? mqAll.size() / cidAll.size() + 1 : mqAll.size() / cidAll.size()
         *          mod > 0 && index < mod --> 队列的数量不是消费者数量的整数倍并且currentCID是处在消费者靠前的位置, 可以多分一个
         * 如果队列小于等于消费者
         *      averageSize = 1
         */
        int averageSize = mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                        + 1 : mqAll.size() / cidAll.size());
        /*
         * mod > 0 && index < mod
         *      举例: mqSize = 10, cidSize = 4, index = 1
         *          startIndex = 1 * averageSize
         * mod > 0 && index < mod
         *      举例: mqSize = 10, cidSize = 4, index = 3
         *          startIndex = 3 * averageSize + 2
         *      举例: mqSize = 10, cidSize = 5, index = 1
         *          startIndex = 3 * averageSize + 0
         */
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
