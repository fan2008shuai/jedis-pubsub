package org.fan.test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisResource {
    private static String LOCK = "lock";
    private static JedisPool jedisPool;

    private static JedisPoolConfig getJedisPoolConfig() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxActive(1);
        config.setMaxWait(2000);
//        config.setMaxTotal(1);
//        config.setMaxWaitMillis(2000);
        config.setMaxIdle(5);
        config.setMinIdle(1);

        return config;
    }

    public static JedisPool getJedisPool() {
        if (jedisPool == null) {
            synchronized (LOCK) {
                if (jedisPool == null) {
                    jedisPool = new JedisPool(getJedisPoolConfig(),"127.0.0.1", 6379);
                }
            }
        }
        return jedisPool;
    }

    public static Jedis getJedis() {
        return getJedisPool().getResource();
    }

}
