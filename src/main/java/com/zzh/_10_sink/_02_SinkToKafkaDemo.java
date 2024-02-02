package com.zzh._10_sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * 数据走向：
 *      控制台 -> Flink -> Kafka
 * 使用步骤：
 *      nc -lk 7777
 *      kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic topic1
 *      kafka-topics --bootstrap-server localhost:9092 --list
 *      kafka-console-consumer --bootstrap-server localhost:9092 --topic topic1
 */
public class _02_SinkToKafkaDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 如果是精准一次，必须开启checkpoint（后续章节介绍）
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        SingleOutputStreamOperator<String> source = env.socketTextStream("localhost", 7777);
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                // 指定 kafka 的地址和端口
                .setBootstrapServers("localhost:9092")
                // 指定序列化器：指定Topic名称、具体的序列化
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("topic1")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                // 写到kafka的一致性级别： 精准一次、至少一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 如果是精准一次，必须设置 事务的前缀
                .setTransactionalIdPrefix("test-")
                // 如果是精准一次，必须设置 事务超时时间: 大于checkpoint间隔，小于 max 15分钟
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10 * 60 * 1000 + "")
                .build();
        source.sinkTo(kafkaSink);
        env.execute();
    }
}
