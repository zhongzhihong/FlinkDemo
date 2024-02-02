package com.zzh._04_aggregation;

import com.zzh.entity.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 简单聚合（sum、min、max、minBy、maxBy）
 */
public class _02_SimpleAggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );
        KeyedStream<WaterSensor, String> keyedBy = waterSensorDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor waterSensor) throws Exception {
                return waterSensor.getId();
            }
        });
        /*
        WaterSensor(id=s1, ts=1, vc=1)
        WaterSensor(id=s1, ts=1, vc=12)
        WaterSensor(id=s2, ts=2, vc=2)
        WaterSensor(id=s3, ts=3, vc=3)
         */
        keyedBy.sum("vc").print();
        /*
        WaterSensor(id=s1, ts=1, vc=1)
        WaterSensor(id=s1, ts=1, vc=11)
        WaterSensor(id=s2, ts=2, vc=2)
        WaterSensor(id=s3, ts=3, vc=3)
         */
        keyedBy.max("vc").print();
        /*
        WaterSensor(id=s1, ts=1, vc=1)
        WaterSensor(id=s1, ts=11, vc=11)
        WaterSensor(id=s2, ts=2, vc=2)
        WaterSensor(id=s3, ts=3, vc=3)
        由结果分析可知 max（min） 和 maxBy（minBy） 的区别：
            max：只会取比较字段的最大值，非比较字段保留第一次的值
            maxBy：取比较字段的最大值，同时非比较字段取最大值这条数据的值
         */
        keyedBy.maxBy("vc").print();
        env.execute();
    }
}
