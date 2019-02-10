package org.tydhot.flink.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaDataProducer {

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers", "localhost:9092");

        KafkaProducer producer = new KafkaProducer<Integer, String>(properties);
        while(true) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("test", String.valueOf((int)(1+Math.random()*3500)));
            producer.send(record, (recordMetadata, e) -> {
                System.out.println("send massage is " + record.value());
            });
            Thread.sleep(1000L);
        }
    }

}
