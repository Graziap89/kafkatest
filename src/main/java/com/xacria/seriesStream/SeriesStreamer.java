package com.xacria.seriesStream;


import com.xacria.model.Record;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class SeriesStreamer {
    public static void main(String[] args) throws Exception {
        String inputTopic = "inputSeriesTopic";
        String outputSeriesTopic = "outputSeriesTopic";

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "seriesStream"+System.currentTimeMillis());
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        StreamsBuilder builder = new StreamsBuilder();
        builder.<String,String>stream(inputTopic)

                .filter((key, value) -> value.contains("2019"))
                .map((key, value) -> {
                    Record record = Record.getRecordFromLog(value);
                    return KeyValue.pair(record.title + "_" + record.date, record.value);
                })
                .peek((k,v)-> System.out.println("<" + k + "> , <" + v + ">"))
                .to(outputSeriesTopic);


        //KStream<String, String> stream = builder.<String,String>stream(inputTopic).filter((key, value) ->value.contains("2019"));

        /*KStream<String, String> transformed = stream
                .filter((key, value) -> value.contains("2019"))
                .map((key, value) -> {
                    Record record = Record.getRecordFromLog(value);
                   return KeyValue.pair(record.title + "_" + record.date, record.value);
                })

                .peek((k,v)-> System.out.println("<" + k + "> , <" + v + ">"));
        transformed.to("outputSeriesTopic");
*/
        /*
        String outputTopic = "outputSeriesTopic";
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();
        wordCounts.to(stringSerde, longSerde, outputTopic);
         */


        KafkaStreams streams = new KafkaStreams(builder.build(),
                streamsConfiguration);

        streams.start();

        Thread.sleep(50000);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
        }));

    }
}
