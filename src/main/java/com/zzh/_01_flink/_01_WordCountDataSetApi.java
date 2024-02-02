package com.zzh._01_flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class _01_WordCountDataSetApi {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.读取文件数据
        DataSource<String> textFile = env.readTextFile("input/text.txt");

        // 3.按行切分并转换数据 --> (word,1)
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = textFile.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // 3.1 按照空格切分单词
                String[] words = value.split(" ");
                // 3.2 转换单词为(word,1)
                for (String word : words) {
                    Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);
                    // 3.3 调用采集器往下游发送数据
                    collector.collect(wordTuple2);
                }
            }
        });

        // 4.按照单词分组（传的是单词的索引值：第一个元素）
        UnsortedGrouping<Tuple2<String, Integer>> grouped = wordAndOne.groupBy(0);

        // 5.各分组聚合（传的是聚合的索引值：第二个元素）
        AggregateOperator<Tuple2<String, Integer>> result = grouped.sum(1);

        // 6.输出
        result.print();
    }
}
