package org.tydhot.flink.producer;

import redis.clients.jedis.Jedis;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class RedisDataProducer {

    public static void main(String[] args) {
        RedisDataProducer redisDataProducer = new RedisDataProducer();
        redisDataProducer.deal();
    }

    public void deal() {
        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        for(int i = 0; i < 100; i++) {
            threadPoolExecutor.execute(new RedisWorker(i));
        }
    }

    class RedisWorker implements Runnable{
        Integer count;

        public RedisWorker(Integer count) {
            this.count = count;
        }

        @Override
        public void run() {
            Jedis jedis = new Jedis("localhost", 6379);
            for(int j = 0; j < 100; j++) {
                jedis.append(String.valueOf(count * 100 + j), String.valueOf((int)(1+Math.random()*10000)));
            }
            jedis.close();
        }
    }

}
