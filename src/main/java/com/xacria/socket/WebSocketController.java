package com.xacria.socket;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Component
public class WebSocketController implements Runnable{
    @Autowired
    private SimpMessagingTemplate template;


    String topic = "outputSeriesTopic";
    Map<String,Object> map=new HashMap<>();
    KafkaConsumer<String, String> consumer;


    public void startWebSocket(){
        new Thread(this).start();

    }

    @PostConstruct
    public void postConstruct(){
        map.put("bootstrap.servers","localhost:9094");
        map.put("group.id","MyGroupId"+System.currentTimeMillis());
        map.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        map.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(map);
        consumer.subscribe(Collections.singletonList(topic));
        System.out.print("Start websocket");

        startWebSocket();
    }

    @Override
    public void run(){
        int i = 0;
        while (true){
            try{
                //template.convertAndSend("/topic/hello",i);    i+=1;

                ConsumerRecords<String,String> consumerRecords=consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> message : consumerRecords) {
                    template.convertAndSend("/topic/hello", message.key() + ","+message.value());
                    System.out.println("Message received: <" + message.key() + ", <"+message.value()+">");
                }

                Thread.sleep(1000);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }
}
