package com.zzh._02_source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * 从集合中读取数据
 */
public class _01_CollectionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6)); // 数组转集合
        source.print();
        env.execute();
    }
}
