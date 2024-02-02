package com.zzh._02_source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class _04_DataGeneratorDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /*
        需要引入依赖：flink-connector-datagen
        数据生成器四个参数：
            第一个：GeneratorSource接口，需要实现，重写map方法，输入类型固定是Long
            第二个：long类型，自动生成的数字序列（从0自增）的最大值，达到这个值就会停止
            第三个：限速策略，比如每秒生成几条数据
            第四个：返回的类型
         */
        DataGeneratorSource<String> generatorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return "Number:" + value;
                    }
                },
                // 传`可达的数字`则是有界流，如果传的值为`Long.MAX_VALUE`则是无界流
                10,
                RateLimiterStrategy.perSecond(1),
                Types.STRING
        );
        env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "DataGeneratorSource").print();
        env.execute();
    }
}
