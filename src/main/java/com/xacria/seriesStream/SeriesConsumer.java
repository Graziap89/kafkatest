package com.xacria.seriesStream;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;

public class SeriesConsumer {
    public static void main(String[] args) throws Exception {

        String topic = "outputSeriesTopic";
        Map<String,Object> map=new HashMap<>();
        map.put("bootstrap.servers","localhost:9094");
        map.put("group.id","MyGroupId2");
        map.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        map.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(map);
        consumer.subscribe(Collections.singletonList(topic));

        try {
            while (true) {
                ConsumerRecords<String,String> consumerRecords=consumer.poll(100);
                consumerRecords.forEach(record ->
                        System.out.println(record.key() + ". " + record.value()));
            }
        } finally {
            consumer.close();
        }
    }
}
