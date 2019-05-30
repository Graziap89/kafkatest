package com.xacria.seriesStream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.*;

public class SeriesConsumer {
    public static void main(String[] args){

        String topic = "outputSeriesTopic";
        Map<String,Object> map=new HashMap<>();
        map.put("bootstrap.servers","localhost:9094");
        map.put("group.id","MyGroupId"+System.currentTimeMillis());
        map.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        map.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(map);
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
                ConsumerRecords<String,String> consumerRecords=consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> message : consumerRecords) {
                System.out.println("Message received: <" + message.key() + ", <"+message.value()+">");
            }
        }
        //Runtime.getRuntime().addShutdownHook(new Thread(() -> { consumer.close(); }));
    }
}
