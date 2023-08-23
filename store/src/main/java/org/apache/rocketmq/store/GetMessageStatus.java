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
package org.apache.rocketmq.store;

public enum GetMessageStatus {

    /**
     * 表示消息已经被找到并返回
     */
    FOUND,

    NO_MATCHED_MESSAGE,

    /**
     * 表示消息正在被移除
     */
    MESSAGE_WAS_REMOVING,

    /**
     * 表示消息的偏移量为空
     */
    OFFSET_FOUND_NULL,

    /**
     * 表示消息的偏移量溢出
     */
    OFFSET_OVERFLOW_BADLY,

    /**
     * 表示消息的偏移量超出了一个文件的大小
     */
    OFFSET_OVERFLOW_ONE,

    /**
     * 表示消息的偏移量太小
     */
    OFFSET_TOO_SMALL,

    /**
     * 表示没有匹配的逻辑队列
     */
    NO_MATCHED_LOGIC_QUEUE,

    /**
     * 表示队列中没有消息
     */
    NO_MESSAGE_IN_QUEUE,
}
