package com.example.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class SimpleStreamApplication {
    private static String APPLICATION_NAME = "streams-application";
    private static String BOOTSTRAP_SERVER = "my-kafka:9092";
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_COPY = "stream_log_copy";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 스트림 토폴로지를 정의하기 위한 용도
        StreamsBuilder builder = new StreamsBuilder();
        // stream_log로부터 KStream 객체를 만들기위해 StreamsBuilder.stream() 메서드 사용
        // stream() 외에 KTable을 만드는 table(), globalKTable() 메서드도 지원 < 이 메소드들은 최초의 토픽 데이터를 가져오는 소스 프로세서이다.
        KStream<String, String> stream = builder.stream(STREAM_LOG);

        // stream_log 토픽을 담은 KStream 객체를 다른 토픽으로 전송하기위해 to() 실행
        // to() 메서드는 KStream 인스턴스의 데이터들을 특정 토픽으로 저장하기 위한 용도로 사용 < 싱크 프로세서
        stream.to(STREAM_LOG_COPY);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
