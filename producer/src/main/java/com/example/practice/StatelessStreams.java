package com.example.practice;

import com.example.processor.FilterProcessor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class StatelessStreams {

    private static String APPLICATION_NAME = "state-application";
    private static String BOOTSTRAP_SERVER = "10.107.13.123:9092";
    public static void main(String[] args) {
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> textLines = builder.stream("streams-plaintext-input", Consumed.with(stringSerde, stringSerde));

        KTable<String, Long> wordCounts = textLines
                .peek((k, v) -> System.out.println(k + " ::: " + v))
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split(" ")))
                .groupBy((key, value) -> value)
//                .windowedBy(TimeWindows.of(Duration.ofSeconds(10)).grace(Duration.ZERO))
                .count();


        wordCounts.toStream()
                .peek((k, v) -> System.out.println(k + " ::: " + v))
                .to("streams-wordcount-output", Produced.with(stringSerde, longSerde));

        KafkaStreams streaming = new KafkaStreams(builder.build(), props);
        streaming.start();
    }
}
