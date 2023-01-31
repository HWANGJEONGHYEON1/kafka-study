package com.example.processor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class SimpleKafkaProcessor {

    private static String APPLICATION_NAME = "processor-application";
    private static String BOOTSTRAP_SERVER = "my-kafka:9092";
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_FILTER = "stream_log_filter";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 프로세서 API를 사용한 토폴로지를 구성하기 위해 사용된다.
        Topology topology = new Topology();
        // stream_log 토픽을 소스 프로세서로 가져오기 위해 addSource() 사용
        topology.addSource("Source", STREAM_LOG)
                // 1번째 인자는 스트림 프로세스 이름, 2번 째 인자는 사용자가 정의한 프로세서 인스턴스
                // 3번째 인자는 부모 노드를 입력 Source 즉 addSource를 통해 선언한 소스프로세서의 다음 프로세서는 Process 스트림 프로세서이다.
                .addProcessor("Process",
                        FilterProcessor::new,
                        "Source")
                //
                .addSink("Sink",
                        STREAM_LOG_FILTER,
                        "Process");

        KafkaStreams streaming = new KafkaStreams(topology, props);
        streaming.start();
    }

}
