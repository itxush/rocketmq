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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    private boolean sendLatencyFaultEnable = false;

    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        // 发送延迟故障启用，默认为false
        if (this.sendLatencyFaultEnable) {
            try {
                // 获取一个index
                int index = tpInfo.getSendWhichQueue().incrementAndGet();
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    // 选取的这个broker是可用的 直接返回
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName()))
                        return mq;
                }
                // 到这里找了一圈还是没有找到可用的broker
                // 选择距离可用时间最近的
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                /*
                 * 找到这个broker之后然后根据这个broker name 获取写队列的个数 其实你这个写队列个数有几个, 你这个broker对应的MessageQueue就有几个
                 * 如果write size >0的话就是这个broker 不是null
                 * 就找一个mq，然后设置上它的broker name 与queue id
                 * 如果write<=0，直接移除这个broker对应FaultItem，最后实在是找不到就按照上面那种普通方法来找了。
                 */
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().incrementAndGet() % writeQueueNums);
                    }
                    return mq;
                } else {
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            return tpInfo.selectOneMessageQueue();
        }

        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     * 更新延迟故障信息
     *
     * @param brokerName     broker名称
     * @param currentLatency 延迟时间
     * @param isolation      该broker是否需要隔离
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        // 是否开启延迟故障容错
        if (this.sendLatencyFaultEnable) {
            // 计算不可用持续时间
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            // 存储
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    /**
     * 计算[选择]不可用持续时间
     *
     * 根据broker的响应延迟时间, 选择或者说人为认定该broker不能提供服务的持续时间
     * 比如: 某个broker延迟越高, 可能是其负载很高, 所以提高该broker的不可用持续时间, 降低它的负载
     *
     * @param currentLatency 当前延迟
     * @return 不可用持续时间
     */
    private long computeNotAvailableDuration(final long currentLatency) {
        // private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
        // private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};
        // 倒着遍历
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                // 如果延迟大于某个时间，就返回对应服务不可用时间，可以看出来，响应延迟100ms以下是没有问题的
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
