package org.tydhot.flink.handler;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import redis.clients.jedis.Jedis;

import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

public class FlinkHandler {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSink<Tuple3<Long, Long, Integer>> dataStream = streamExecutionEnvironment
                .addSource(new SourceFunction<String>() {
                    private Boolean goon = true;

                    @Override
                    public void run(SourceContext sourceContext) throws Exception {
                        Properties properties = new Properties();
                        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                        properties.put("bootstrap.servers", "localhost:9092");
                        properties.put("enable.auto.commit", "true");
                        properties.put("auto.commit.interval.ms", "1000");
                        properties.put("group.id", "test");
                        properties.put("session.timeout.ms", "30000");

                        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
                        consumer.subscribe(Collections.singletonList("test"));
                        while (goon) {
                            ConsumerRecords<String, String> records = consumer.poll(1000);
                            for (ConsumerRecord<String, String> record : records) {
                                sourceContext.collect(record.value());
                            }
                        }
                    }

                    @Override
                    public void cancel() {
                        goon = false;
                    }
                })
                .timeWindowAll(Time.seconds(5))
                .apply(new MyTuple3Applier())
                .filter((FilterFunction<Tuple3<Long, Long, Integer>>) tuple3 -> {
                    System.out.println("now there is " + tuple3.f2 + " to be filter");
                    Jedis jedis = new Jedis("localhost", 6379);
                    if (jedis.exists(String.valueOf(tuple3.f2))) {
                        jedis.close();
                        return true;
                    }
                    jedis.close();
                    return false;
                })
                .map((MapFunction<Tuple3<Long, Long, Integer>, Tuple3<Long, Long, Integer>>) tuple3 -> {
                    Jedis jedis = new Jedis("localhost", 6379);
                    String result = jedis.get(String.valueOf(tuple3.f2));
                    jedis.close();
                    return new Tuple3<Long, Long, Integer>(tuple3.f0, tuple3.f1, Integer.parseInt(result));
                })
                .returns(Types.TUPLE(Types.LONG, Types.LONG, Types.INT))
                .addSink(new SinkFunction<Tuple3<Long, Long, Integer>>() {
                    @Override
                    public void invoke(Tuple3<Long, Long, Integer> value) throws Exception {
                        ConnectionFactory connectionFactory = new ConnectionFactory();
                        connectionFactory.setHost("localhost");
                        connectionFactory.setPort(5672);
                        connectionFactory.setPassword("123456");
                        connectionFactory.setUsername("root");
                        Connection connection = connectionFactory.newConnection();
                        Channel channel = connection.createChannel();
                        try {
                            channel.queueDeclare("test", false, false, false, null);

                            StringBuilder result = new StringBuilder();
                            result.append("start time : " + new Date(value.f0)).append(" ;");
                            result.append("end time : " + new Date(value.f1)).append(" ;");
                            result.append("valid redult  : " + value.f2).append(" ;");

                            channel.basicPublish("", "test", null, result.toString().getBytes("utf-8"));
                        } finally {
                            channel.close();
                            connection.close();
                        }
                    }
                });

        streamExecutionEnvironment.execute();
    }

    public static class MyTuple3Applier implements AllWindowFunction<String, Tuple3<Long, Long, Integer>, TimeWindow> {

        @Override
        public void apply(TimeWindow window, Iterable<String> values, Collector<Tuple3<Long, Long, Integer>> out) throws Exception {
            Iterator<String> iterator = values.iterator();
            Integer sum = 0;
            while(iterator.hasNext()) {
                sum += Integer.parseInt(iterator.next());
            }
            out.collect(new Tuple3<Long, Long, Integer>(window.getStart(), window.getEnd(), sum));
        }
    }

}
