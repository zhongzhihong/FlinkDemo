package com.zzh._02_source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 本地测试：
 *  创建主题：kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic topic1
 *  查看主题：kafka-topics --bootstrap-server localhost:9092 --list
 *  生产数据：kafka-console-producer --bootstrap-server localhost:9092 --topic topic1
 */
public class _03_KafkaSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 需要引入依赖：flink-connector-kafka
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setGroupId("test")
                .setTopics("topic1")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                /*
                Flink：
                    earliest：从 最早 开始消费
                    latest  ：从 最新 开始消费
                Kafka：
                    earliest：如果有offset，从offset继续消费，没有则从 最早 消费
                    latest  ：如果有offset，从offset继续消费，没有则从 最新 消费
                 */
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource").print();
        env.execute();
    }
}
