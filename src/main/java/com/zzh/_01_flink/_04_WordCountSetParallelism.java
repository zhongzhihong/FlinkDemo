package com.zzh._01_flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class _04_WordCountSetParallelism {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        // 为了方便本地测试并行度，可以通过以下api让代码在运行时生成WebUI界面（需要引入依赖：flink-runtime-web，访问地址：localhost:8081）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        // 全局设置并行度
        env.setParallelism(3);

        // 2.读取数据
        // 启动本地被监听端口命令：nc -lk 7777
        DataStreamSource<String> socketDS = env.socketTextStream("127.0.0.1", 7777);

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
                // 算子级别设置并行度，测试得知，算子设置的优先级高于全局设置的优先级
                .setParallelism(2)
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
