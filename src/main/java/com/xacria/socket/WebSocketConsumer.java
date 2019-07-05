package com.xacria.socket;

import com.xacria.MongoDB.MongodbCustom;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class WebSocketConsumer implements Runnable {
   private final SimpMessagingTemplate template;
   private final MongodbCustom db;
    private final KafkaConsumer<String, String> consumer;

    public WebSocketConsumer(String topic, SimpMessagingTemplate template){
        db = new MongodbCustom();
        db.connect();
        this.template = template;
        consumer = new KafkaConsumer<>(init());
        consumer.subscribe(Collections.singletonList(topic));

    }

    private Map<String,Object> init(){
        Map<String,Object> map=new HashMap<>();
        map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,AppConfig.BOOTSTRAP_SERVERS);
        map.put(ConsumerConfig.GROUP_ID_CONFIG,"MyGroupId"+System.currentTimeMillis());
        map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        return map;

    }
    public void run(){
        while (true){
            try{
                ConsumerRecords<String,String> consumerRecords = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> message : consumerRecords) {
                    template.convertAndSend("/topic/hello", message.key() + ","+message.value());
                    System.out.println("Message received: <" + message.key() + ">, <"+message.value()+">");
                    db.insert(message.key(), message.value());
                }
                Thread.sleep(1000);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }
}
