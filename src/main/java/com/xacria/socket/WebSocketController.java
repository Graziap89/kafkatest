package com.xacria.socket;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class WebSocketController implements Runnable{
    @Autowired
    private SimpMessagingTemplate template;

    public void startWebSocket(){
        new Thread(this).start();

    }

    @PostConstruct
    public void postConstruct(){
        System.out.print("Start websocket");
        startWebSocket();

    }

    @Override
    public void run(){
        int i = 0;
        while (true){
            try{
                template.convertAndSend("/topic/hello",i);
                i+=1;
                Thread.sleep(1000);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }
}
