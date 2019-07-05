package com.xacria.seriesStream;

import com.xacria.model.Record;
import com.xacria.socket.AppConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class SeriesStreamer implements Runnable {
    private final KafkaStreams streams;

    public SeriesStreamer(String inputTopic, String outputSeriesTopic ){
        StreamsBuilder builder = new StreamsBuilder();
        builder.<String,String>stream(inputTopic)

                .filter((key, value) -> value.contains("2019"))
                .map((key, value) -> {
                    Record record = Record.getRecordFromLog(value);
                    return KeyValue.pair(record.title + "_" + record.date, record.value);
                })
                .peek((k,v)-> System.out.println("<" + k + "> , <" + v + ">"))
                .to(outputSeriesTopic);

        streams = new KafkaStreams(builder.build(),
                init());

    }

    private Properties init(){
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "seriesStream"+System.currentTimeMillis());
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    return streamsConfiguration;
    }

    public void run(){
        streams.start();
    }
  }
