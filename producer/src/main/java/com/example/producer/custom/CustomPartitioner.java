package com.example.producer.custom;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (keyBytes == null) {
            throw new InvalidRecordException("Need MessageKey");
        }

        if (((String) key).equals("Pangyo")) {
            return 0;
        }

        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        int numberPartitions =  partitionInfos.size();

        return Utils.toPositive(Utils.murmur2(keyBytes)) % numberPartitions;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
