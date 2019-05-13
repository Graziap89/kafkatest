package com.xacria.kafkatest;

//import util.properties packages
import java.util.Properties;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Date;

import org.apache.kafka.common.serialization.StringSerializer;

public class FirstProducer {
    public static void main(String[] args) throws Exception{


        //Assign topicName to string variable
        String topicName = "inputTopic";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9094");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer
                <String, String>(props);

        for(int i = 0; i < 10; i++)
            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(i), Integer.toString(i*10)));
        System.out.println("Message sent successfully");
        producer.close();
    }
}
