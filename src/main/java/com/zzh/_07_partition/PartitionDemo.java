package com.zzh._07_partition;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 物理分区算子：随机（shuffle）、轮询（rebalance）、重缩放（rescale）、 广播（broadcast）、全局（global）、自定义（custom）
 */
public class PartitionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 7777);
        // shuffle：随机分区。
        socketTextStream.shuffle().print();
        // rebalance：轮询分区。
        socketTextStream.rebalance().print();
        // rescale：缩放分区。同样实现轮询，但是局部组队，更高效
        socketTextStream.rescale().print();
        // broadcast：广播分区。发送给所有下游
        socketTextStream.broadcast().print();
        // global：全局分区。只发往第一个子任务
        socketTextStream.global().print();
        // partitionCustom：自定义分区。
        socketTextStream.partitionCustom(new MyPartitioner(), new KeySelector<String, String>() {
            @Override
            public String getKey(String s) throws Exception {
                return s;
            }
        }).print();
        env.execute();
    }
}
