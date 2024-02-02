package com.zzh._09_combine;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 合流（union）：流的类型必须一致。
 */
public class _01_UnionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3);
        DataStreamSource<Integer> source2 = env.fromElements(11, 22, 33);
        DataStreamSource<String> source3 = env.fromElements("111", "222", "333");
        // 分开合并
        source1.union(source2)
                .union(source3.map(s -> Integer.valueOf(s)))
                .print();
        // 一次合并
        source1.union(source2, source3.map(s -> Integer.valueOf(s))).print();
        env.execute();
    }
}
