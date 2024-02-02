package com.zzh._01_flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class _03_WordCountDataFromSocket {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.读取数据
        // 启动本地被监听端口命令：nc -lk 7777
        DataStreamSource<String> socketDS = env.socketTextStream("172.16.84.140", 7777);

        // 3.处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = socketDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);
                            collector.collect(wordTuple2);
                        }
                    }
                })
                // 这里需要手动指明返回的值类型，不然由于java中的泛型擦除会导致报错
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .sum(1);

        // 4.输出
        result.print();

        // 5.执行
        env.execute();
    }
}
