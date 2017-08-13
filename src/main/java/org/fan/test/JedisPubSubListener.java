package org.fan.test;

import redis.clients.jedis.JedisPubSub;
import java.util.concurrent.CountDownLatch;


public class JedisPubSubListener extends JedisPubSub {
    private CountDownLatch subCountDownLatch;
    private CountDownLatch unsubCountDownLatch;

    public void setSubCountDownLatch(CountDownLatch subCountDownLatch) {
        this.subCountDownLatch = subCountDownLatch;
    }

    public void setUnsubCountDownLatch(CountDownLatch unsubCountDownLatch) {
        this.unsubCountDownLatch = unsubCountDownLatch;
    }

    public void onMessage(String s, String s1) {

    }

    public void onPMessage(String s, String s1, String s2) {

    }

    public void onSubscribe(String s, int i) {
        subCountDownLatch.countDown();
        System.out.println("sub: " + s + " channel count: " + i);
    }

    public void onUnsubscribe(String s, int i) {
        unsubCountDownLatch.countDown();
        System.out.println("unsub:  " + s + " channel count: " + i);
        System.out.println("test");
    }

    public void onPUnsubscribe(String s, int i) {

    }

    public void onPSubscribe(String s, int i) {

    }
}
