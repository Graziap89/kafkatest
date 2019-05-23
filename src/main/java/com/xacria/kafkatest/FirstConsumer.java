package com.xacria.kafkatest;
import java.time.Duration;
import java.util.Date;
import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class FirstConsumer {
    public static void main(String[] args) throws Exception {

        String topic = "inputTopic2";
        String group = "ciao"+new Date().getTime();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9094");
        props.put("group.id", group);
        //props.put("enable.auto.commit", "true");
       // props.put("auto.commit.interval.ms", "1000");
        //props.put("session.timeout.ms", "30000");


        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList(topic));
        System.out.println("Subscribed to topic " + topic);
        int i = 0;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s\n",
                        record.offset(), record.key(), record.value());
        }
    }
}
