package com.zzh._03_transform;

import com.zzh.entity.WaterSensor;
import com.zzh.function.MapFunctionImpl;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 基本转换算子-map。
 * 主要用于将数据流中的数据进行转换，形成新的数据流。遵循“一进一出”，即消费一个元素就产出一个元素。
 */
public class _01_MapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );
        // 方式一：匿名内部类
        waterSensorDS.map(new MapFunction<WaterSensor, String>() {
            @Override
            public String map(WaterSensor waterSensor) {
                return waterSensor.getId();
            }
        }).print();
        // 方式二：lambda表达式
        waterSensorDS.map(waterSensor -> waterSensor.getId()).print();
        // 方式三：手动实现MapFunction
        waterSensorDS.map(new MapFunctionImpl()).print();
        env.execute();
    }
}
