package com.zzh._08_split;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 分流（一）
 */
public class FilterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 7777);
        // 方式一：filter。缺点：一个数据要被处理几次，调用多次filter
        socketTextStream.filter(value -> Integer.parseInt(value) % 2 == 0).print("偶数流");
        socketTextStream.filter(value -> Integer.parseInt(value) % 2 == 1).print("奇数流");
        env.execute();
    }
}
