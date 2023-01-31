package com.example.admin;

import com.example.consumer.rebalance.SimpleConsumer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AdminExample {

    private final static Logger logger = LoggerFactory.getLogger(AdminExample.class);


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-kafka:9092");

        AdminClient adminClient = AdminClient.create(configs);

        // 브로커별 설정
        for (Node node : adminClient.describeCluster().nodes().get()) {
            logger.info("node {}", node);
            ConfigResource cr = new ConfigResource(ConfigResource.Type.BROKER, node.idString());
            DescribeConfigsResult describeConfigs = adminClient.describeConfigs(Collections.singleton(cr));
            describeConfigs.all().get()
                    .forEach((broker, config) -> {
                        config.entries().forEach(configEntry -> logger.info(configEntry.name() + "= " + configEntry.value()));
                    });
        }

        // 토픽정보
        Map<String, TopicDescription> test = adminClient.describeTopics(Collections.singleton("test")).all().get();
        logger.info("map {}", test);
        adminClient.close(); // 명시적으로 종료 메서드를 호출하여 리소스 낭비를 하지 않게 만든다.
    }
}
