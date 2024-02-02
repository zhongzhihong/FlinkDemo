package com.zzh._09_combine;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * 合流（connect）：流的类型可以不一致。但是只能两条流合并，不能多条流。
 */
public class _02_ConnectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3);
        DataStreamSource<String> source2 = env.fromElements("11", "22", "33");
        // connect 后，流类型变成：ConnectedStreams
        ConnectedStreams<Integer, String> connect = source1.connect(source2);
        // map 后，流类型又变回：DataStreamSource
        SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "来源于数字流" + value.toString();
            }

            @Override
            public String map2(String value) throws Exception {
                return "来源于字母流" + value;
            }
        });
        map.print();
        env.execute();
    }
}
