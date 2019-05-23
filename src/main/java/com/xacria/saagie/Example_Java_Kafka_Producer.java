package com.xacria.saagie;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Example_Java_Kafka_Producer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // Instanciation du KafkaProducer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // Write
        do {
            // create the topic message, enter topic name and message
            ProducerRecord<String, String> record = new ProducerRecord<>("TopicJAVA", produceData());
            // function to push the message to kafka topic
            SendToKafka(producer, record);
            Thread.sleep(500);
        } while (true);
    }
    // Get random US City Name
    private static String getUsCity() {
        String[] list = { "Montgomery", "Juneau", "Phoenix", "Little Rock", "Sacramento", "Raleigh", "Columbia", "Denver",
                "Hartford", "Bismarck", "Pierre", "Dover", "Tallahassee", "Atlanta", "Honolulu", "Boise", "Springfield",
                "Indianapolis", "Des Moines", "Topeka", "Frankfort", "Baton Rouge", "Augusta", "Annapolis", "Boston", "Lansing",
                "Saint Paul", "Jackson", "Jefferson City", "Helena", "Lincoln", "Carson City", "Concord", "Trenton", "Santa Fe",
                "Albany", "Columbus", "Oklahoma City", "Salem", "Harrisburg", "Providence", "Nashville", "Austin",
                "Salt Lake City", "Montpelier", "Richmond", "Charleston", "Olympia", "Madison", "Cheyenne" };
        Integer random = ((Long) Math.round(Math.random() * (list.length - 1))).intValue();
        return list[random];
    }

    // Get message
    private static String produceData() {
        return getUsCity();
    }

    // Create Kafka producer with parameters
    private static KafkaProducer<String, String> createKafkaProducer() {
        Map<String,Object> map=new HashMap<>();
        map.put("bootstrap.servers","localhost:9094");
        map.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        map.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(map);
    }

    // Send message to Topic in Kafka
    private static void SendToKafka(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        try {
            producer.send(record, new ProducerCallback());
        } catch (Exception e) { e.printStackTrace(); }
    }

    // Method to see if the message is send, if no exception the message is send
    private static class ProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e!=null)
            {
                e.printStackTrace();
            } else {
                System.out.println("sent on : " + recordMetadata.topic() + " offset : " + recordMetadata.offset());
            }

        }
    }

}
