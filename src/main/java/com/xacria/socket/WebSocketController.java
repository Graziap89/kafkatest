package com.xacria.socket;

import com.xacria.seriesStream.SeriesStreamer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class WebSocketController implements Runnable{
    @Autowired
   private SimpMessagingTemplate template;

    @PostConstruct
    public void postConstruct(){
        new Thread(this).start();
    }

    @Override
    public void run(){
        SeriesStreamer streamer = new SeriesStreamer(AppConfig.INPUT_TOPIC, AppConfig.OUTPUT_TOPIC);
        WebSocketConsumer consumer = new WebSocketConsumer(AppConfig.OUTPUT_TOPIC, template);
        new Thread(streamer).start();
        new Thread(consumer).start();
    }
}
