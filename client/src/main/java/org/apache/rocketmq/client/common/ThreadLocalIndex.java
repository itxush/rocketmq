package org.apache.rocketmq.client.common;

import java.util.Random;

public class ThreadLocalIndex {
    /**
     * 可以看到这个sendWhichQueue是用ThreadLocal实现的
     * 然后这个样子就可以一个线程一个index，而且不会出现线程安全问题。
     */
    private final ThreadLocal<Integer> threadLocalIndex = new ThreadLocal<Integer>();
    private final Random random = new Random();
    private final static int POSITIVE_MASK = 0x7FFFFFFF;

    public int incrementAndGet() {
        Integer index = this.threadLocalIndex.get();
        // 如果不存在就创建  然后设置到threadLocalIndex中
        if (null == index) {
            index = Math.abs(random.nextInt());
            this.threadLocalIndex.set(index);
        }
        /*
         * 这个index 其实某个线程内自增1的，这样就形成了某个线程内轮询的效果。
         * 这个样子的话，同步发送其实就是单线程的轮询
         * 异步发送就是多个线程并发发送，然后某个线程内轮询
         */
        this.threadLocalIndex.set(++index);
        return Math.abs(index & POSITIVE_MASK);
    }

    @Override
    public String toString() {
        return "ThreadLocalIndex{" +
            "threadLocalIndex=" + threadLocalIndex.get() +
            '}';
    }
}
