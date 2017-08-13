package org.fan.test;

import redis.clients.jedis.Client;
import redis.clients.jedis.Connection;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.RedisInputStream;
import redis.clients.util.SafeEncoder;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static redis.clients.jedis.Protocol.Keyword.*;
import static redis.clients.jedis.Protocol.Keyword.PSUBSCRIBE;
import static redis.clients.jedis.Protocol.Keyword.PUNSUBSCRIBE;


public class JedisPubSubListener extends JedisPubSub {
    private CountDownLatch subCountDownLatch;
    private CountDownLatch unsubCountDownLatch;

    private Client client;

    public void setSubCountDownLatch(CountDownLatch subCountDownLatch) {
        this.subCountDownLatch = subCountDownLatch;
    }

    public void setUnsubCountDownLatch(CountDownLatch unsubCountDownLatch) {
        this.unsubCountDownLatch = unsubCountDownLatch;
    }

    public void proceed(Client client, String... channels) {
        this.client = client;
        client.subscribe(channels);
        flush(client);
        process(client);
    }

    private void flush(Client client) {
        try {
            Method method = Connection.class.getDeclaredMethod("flush", new Class[]{});
            method.setAccessible(true);
            method.invoke(client, new Class[]{});
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    public void unsubscribe(String... channels) {
        client.unsubscribe(channels);
        flush(client);
    }

    private void process(Client client) {
        int subscribedChannels = 0;
        RedisInputStream inputStream = null;
        do {

            try {
                Field field = Connection.class.getDeclaredField("inputStream");
                field.setAccessible(true);
                inputStream = (RedisInputStream) field.get(client);
            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            List<Object> reply = (List<Object>) Protocol.read(inputStream);
//            List<Object> reply = client.getObjectMultiBulkReply();
            final Object firstObj = reply.get(0);
            if (!(firstObj instanceof byte[])) {
                throw new JedisException("Unknown message type: " + firstObj);
            }
            final byte[] resp = (byte[]) firstObj;
            if (Arrays.equals(SUBSCRIBE.raw, resp)) {
                subscribedChannels = ((Long) reply.get(2)).intValue();
                final byte[] bchannel = (byte[]) reply.get(1);
                final String strchannel = (bchannel == null) ? null
                        : SafeEncoder.encode(bchannel);
                onSubscribe(strchannel, subscribedChannels);
            } else if (Arrays.equals(UNSUBSCRIBE.raw, resp)) {
                subscribedChannels = ((Long) reply.get(2)).intValue();
                final byte[] bchannel = (byte[]) reply.get(1);
                final String strchannel = (bchannel == null) ? null
                        : SafeEncoder.encode(bchannel);
                onUnsubscribe(strchannel, subscribedChannels);
            } else if (Arrays.equals(MESSAGE.raw, resp)) {
                final byte[] bchannel = (byte[]) reply.get(1);
                final byte[] bmesg = (byte[]) reply.get(2);
                final String strchannel = (bchannel == null) ? null
                        : SafeEncoder.encode(bchannel);
                final String strmesg = (bmesg == null) ? null : SafeEncoder
                        .encode(bmesg);
                onMessage(strchannel, strmesg);
            } else if (Arrays.equals(PMESSAGE.raw, resp)) {
                final byte[] bpattern = (byte[]) reply.get(1);
                final byte[] bchannel = (byte[]) reply.get(2);
                final byte[] bmesg = (byte[]) reply.get(3);
                final String strpattern = (bpattern == null) ? null
                        : SafeEncoder.encode(bpattern);
                final String strchannel = (bchannel == null) ? null
                        : SafeEncoder.encode(bchannel);
                final String strmesg = (bmesg == null) ? null : SafeEncoder
                        .encode(bmesg);
                onPMessage(strpattern, strchannel, strmesg);
            } else if (Arrays.equals(PSUBSCRIBE.raw, resp)) {
                subscribedChannels = ((Long) reply.get(2)).intValue();
                final byte[] bpattern = (byte[]) reply.get(1);
                final String strpattern = (bpattern == null) ? null
                        : SafeEncoder.encode(bpattern);
                onPSubscribe(strpattern, subscribedChannels);
            } else if (Arrays.equals(PUNSUBSCRIBE.raw, resp)) {
                subscribedChannels = ((Long) reply.get(2)).intValue();
                final byte[] bpattern = (byte[]) reply.get(1);
                final String strpattern = (bpattern == null) ? null
                        : SafeEncoder.encode(bpattern);
                onPUnsubscribe(strpattern, subscribedChannels);
            } else {
                throw new JedisException("Unknown message type: " + firstObj);
            }
        } while (subscribedChannels > 0);
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
    }

    public void onPUnsubscribe(String s, int i) {

    }

    public void onPSubscribe(String s, int i) {

    }
}
