package com.zzh._01_flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class _02_WordCountDataStreamApi {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.读取文件数据
        DataStreamSource<String> textFile = env.readTextFile("input/text.txt");

        // 3.处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = textFile.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);
                    collector.collect(wordTuple2);
                }
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyedBy = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                // 二元组取第一个元素
                return value.f0;
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedBy.sum(1);

        // 4.输出数据
        result.print();

        // 5.调用执行
        env.execute();
    }
}
