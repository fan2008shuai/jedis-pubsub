package org.fan.test;

import redis.clients.jedis.Jedis;

import java.sql.Time;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by fan.shuai on 17/8/12.
 */
public class JedisPuhSubTest {

    private static CountDownLatch subCountDownLatch;
    private static CountDownLatch unsubCountDownLatch;
    private static JedisPubSubListener listener = new JedisPubSubListener();
    private static Set<String> channels = new HashSet<String>();
    private static volatile Set<String> lastChannels;
    private static ExecutorService executorService = Executors.newSingleThreadExecutor();

    public static void main(String[] args) {
        for (int i = 0; i < 100; i++) {
            if (!acceptNewChannel(i + "")) {
                continue;
            }
            unsubscribeChannels();
            buildCountDownLatch();
            subscribeChannels();
            //unsub old request
        }

    }

    private static void buildCountDownLatch() {
        subCountDownLatch = new CountDownLatch(channels.size());
        unsubCountDownLatch = new CountDownLatch(channels.size());
    }

    private static void unsubscribeChannels() {
        if (lastChannels == null) {
            return;
        }

        try {
            subCountDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        listener.unsubscribe(lastChannels.toArray(new String[0]));
        try {
            unsubCountDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static boolean acceptNewChannel(String channel) {
        if (channels.contains(channel)) {
            return false;
        }
        channels.add(channel);
        return true;
    }

    private static void subscribeChannels() {

        final String[] subChannels = channels.toArray(new String[0]);

        lastChannels = new HashSet<String>();
        lastChannels.addAll(channels);

        listener.setSubCountDownLatch(subCountDownLatch);
        listener.setUnsubCountDownLatch(unsubCountDownLatch);

        executorService.execute(new Runnable() {
            public void run() {
                Jedis jedis = null;
                try {
                    jedis = JedisResource.getJedis();

                    jedis.subscribe(listener, subChannels);
                } catch (Exception e) {
                    //竟然不打印异常
                    e.printStackTrace();
                    if (jedis != null) {
                        JedisResource.getJedisPool().returnBrokenResource(jedis);
                    }
                } finally {
                    if (jedis != null) {
                        JedisResource.getJedisPool().returnResource(jedis);
                    }
                }
            }
        });


    }
}
