package com.xacria.seriesStream;

import com.google.gson.Gson;
import com.xacria.model.Record;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

public class FirstStreamer {
    public static void main(String[] args) throws Exception {
        Gson gson= new Gson();
        String inputTopic = "inputSeriesTopic";

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(
                StreamsConfig.APPLICATION_ID_CONFIG,
                "wordcount-live-test");


        String bootstrapServers = "localhost:9094";
        streamsConfiguration.put(
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);

        streamsConfiguration.put(
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        streamsConfiguration.put(
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());



        //KStreamBuilder builder = new KStreamBuilder(); //NB is deprecated
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stream = builder.stream(
                inputTopic, /* input topic */
                Consumed.with(
                        Serdes.String(), /* key serde */
                        Serdes.String()   /* value serde */
                ));

        KStream<String, String> transformed = stream
                //.filter() //TODO start from here
                .map((key, value) -> {
                    Record record = Record.getRecordFromLog(value);
                   return KeyValue.pair(record.title + "_" + record.date, record.value);
                })
                .peek((k,v)-> System.out.println("<" + k + "> , <" + v + ">"));


        /*
        String outputTopic = "outputSeriesTopic";
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();
        wordCounts.to(stringSerde, longSerde, outputTopic);
         */


        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();

        Thread.sleep(30000);
        streams.close();




    }
}
