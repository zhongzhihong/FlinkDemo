package com.zzh._03_transform;

import com.zzh.entity.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 基本转换算子-flatMap。
 * 主要是将数据流中的整体（一般是集合类型）拆分成一个一个的个体使用。遵循“一进多出”，即消费一个元素可以产生0到多个元素。
 */
public class _03_FlatMapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );
        waterSensorDS.flatMap(new FlatMapFunction<WaterSensor, String>() {
            @Override
            public void flatMap(WaterSensor waterSensor, Collector<String> collector) throws Exception {
                if ("s1".equals(waterSensor.getId())) {
                    collector.collect(waterSensor.getTs().toString());
                } else if ("s2".equals(waterSensor.getId())) {
                    collector.collect(waterSensor.getTs().toString());
                    collector.collect(waterSensor.getVc().toString());
                }
            }
        }).print();
        env.execute();
    }
}
