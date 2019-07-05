package com.xacria.seriesStream;

import com.xacria.socket.AppConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class SeriesProducer implements Runnable{
    private final Producer<String, String> producer;
    private final  String topic;

    public SeriesProducer(String topic){
        this.topic = topic;
        producer = new KafkaProducer<>(init());

    }

    private List<String> generateRandomSeries(){
        List<String> lista= new ArrayList<>();
        StringBuilder stringBuilder=new StringBuilder();
        for(int i = 0; i < 1000; i++){

            stringBuilder
                    .append("serie")
                    .append(((int)Math.floor(Math.random()*4+1)))
                    .append(",")
                    .append(generateRandomInt(2015, 2019))
                    .append("-")
                    .append((int)Math.floor(Math.random()*12)+1)
                    .append("-")
                    .append((int)Math.floor(Math.random()*30)+1)
                    .append(",")
                    .append((int)Math.floor(Math.random()*100));

            lista.add(stringBuilder.toString());
            stringBuilder.delete(0, stringBuilder.length());
        }
        return lista;
    }

    private int generateRandomInt(int min, int max){
        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
    }
    private Properties init(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return props;
    }
    public void run(){
        List<String> lista = generateRandomSeries();
        for(int i = 0; i <lista.size(); i++) {
            producer.send(new ProducerRecord<>(topic, lista.get(i)));
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Message sent successfully");
    }

    public static void main(String[] args) {
        SeriesProducer seriesProducer = new SeriesProducer(AppConfig.INPUT_TOPIC);
        new Thread(seriesProducer).start();

    }
}
