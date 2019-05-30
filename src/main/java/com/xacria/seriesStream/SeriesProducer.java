package com.xacria.seriesStream;

//import util.properties packages

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

//import simple producer packages
//import KafkaProducer packages
//import ProducerRecord packages

public class SeriesProducer {

    SeriesProducer(){}

    public List<String> generateRandomSeries(){
        List<String> lista= new ArrayList<>();
        StringBuilder stringBuilder=new StringBuilder();
        for(int i = 0; i < 100; i++){

            stringBuilder
                    .append("")
                    .append("serie")
                    .append(((int)Math.floor(Math.random()*4+1)))
                    .append(",")
                    .append(generateRandomInt(2000,2019))
                    .append("-")
                    .append((int)Math.floor(Math.random()*12)+1)
                    .append("-")
                    .append((int)Math.floor(Math.random()*30)+1)
                    .append(",")
                    .append((int)Math.floor(Math.random()*100))
                    .append("");

            lista.add(stringBuilder.toString());
            stringBuilder.delete(0, stringBuilder.length());
        }
        return lista;
    }

    private int generateRandomInt(int min, int max){
        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
    }

    public static void main(String[] args) throws Exception{
        SeriesProducer firstProducer = new SeriesProducer();
        List<String> lista =firstProducer.generateRandomSeries();

        //Assign topicName to string variable
        String topicName = "inputSeriesTopic";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9094");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer
                <String, String>(props);

        for(int i = 0; i <lista.size(); i++)
            producer.send(new ProducerRecord<String, String>(topicName,lista.get(i)));
        System.out.println("Message sent successfully");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            producer.close();
        }));
    }
}
