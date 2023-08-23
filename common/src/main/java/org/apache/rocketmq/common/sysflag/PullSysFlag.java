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
package org.apache.rocketmq.common.sysflag;

/**
 * 用来管理 PullRequest 的系统标志位的
 */
public class PullSysFlag {
    private final static int FLAG_COMMIT_OFFSET = 0x1;
    private final static int FLAG_SUSPEND = 0x1 << 1;
    private final static int FLAG_SUBSCRIPTION = 0x1 << 2;
    private final static int FLAG_CLASS_FILTER = 0x1 << 3;
    private final static int FLAG_LITE_PULL_MESSAGE = 0x1 << 4;

    /**
     * 构建系统标志位的
     * <p>
     * 方法首先定义了一个 int 类型的变量 flag，并将其初始化为 0。然后，根据传入的参数，将 flag 的值进行相应的修改。
     * 具体来说
     * 如果 commitOffset 参数为 true，则将 FLAG_COMMIT_OFFSET(1)的值加入到 flag 中；
     * 如果 suspend 参数为 true，则将 FLAG_SUSPEND(1 << 1)的值加入到 flag 中；
     * 如果 subscription 参数为 true，则将 FLAG_SUBSCRIPTION(1 << 2)的值加入到 flag 中；
     * 如果 classFilter 参数为 true，则将 FLAG_CLASS_FILTER(1 << 3)的值加入到 flag 中。
     *
     * @param commitOffset 是否需要提交偏移量
     * @param suspend      是否需要暂停
     * @param subscription 是否需要订阅
     * @param classFilter  是否需要使用类过滤器
     * @return PullRequest的系统标志位
     */
    public static int buildSysFlag(final boolean commitOffset,
                                   final boolean suspend,
                                   final boolean subscription,
                                   final boolean classFilter) {
        int flag = 0;

        if (commitOffset) {
            flag |= FLAG_COMMIT_OFFSET;
        }

        if (suspend) {
            flag |= FLAG_SUSPEND;
        }

        if (subscription) {
            flag |= FLAG_SUBSCRIPTION;
        }

        if (classFilter) {
            flag |= FLAG_CLASS_FILTER;
        }

        return flag;
    }

    public static int buildSysFlag(final boolean commitOffset, final boolean suspend,
                                   final boolean subscription, final boolean classFilter, final boolean litePull) {
        int flag = buildSysFlag(commitOffset, suspend, subscription, classFilter);

        if (litePull) {
            flag |= FLAG_LITE_PULL_MESSAGE;
        }

        return flag;
    }

    public static int clearCommitOffsetFlag(final int sysFlag) {
        return sysFlag & (~FLAG_COMMIT_OFFSET);
    }

    public static boolean hasCommitOffsetFlag(final int sysFlag) {
        return (sysFlag & FLAG_COMMIT_OFFSET) == FLAG_COMMIT_OFFSET;
    }

    public static boolean hasSuspendFlag(final int sysFlag) {
        return (sysFlag & FLAG_SUSPEND) == FLAG_SUSPEND;
    }

    public static boolean hasSubscriptionFlag(final int sysFlag) {
        return (sysFlag & FLAG_SUBSCRIPTION) == FLAG_SUBSCRIPTION;
    }

    public static boolean hasClassFilterFlag(final int sysFlag) {
        return (sysFlag & FLAG_CLASS_FILTER) == FLAG_CLASS_FILTER;
    }

    public static boolean hasLitePullFlag(final int sysFlag) {
        return (sysFlag & FLAG_LITE_PULL_MESSAGE) == FLAG_LITE_PULL_MESSAGE;
    }
}
