package com.zzh._04_aggregation;

import com.zzh.entity.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 聚合算子-keyBy。
 * 基于不同的key，流中的数据将被分配到不同的分区中去；这样一来，所有具有相同的key的数据，都将被发往同一个分区。
 * 在内部，是通过计算key的哈希值（hash code），对分区数进行取模运算来实现的。所以这里key如果是POJO的话，必须要重写hashCode()方法。
 */
public class _01_KeyByDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<WaterSensor> waterSensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );
        /*
        2> WaterSensor(id=s1, ts=1, vc=1)
        1> WaterSensor(id=s2, ts=2, vc=2)
        2> WaterSensor(id=s1, ts=11, vc=11)
        2> WaterSensor(id=s3, ts=3, vc=3)
        运行多次发现：1分区永远是s2、2分区永远是s1和s3。
         */
        waterSensorDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor waterSensor) throws Exception {
                return waterSensor.getId();
            }
        }).print();
        env.execute();
    }
}
