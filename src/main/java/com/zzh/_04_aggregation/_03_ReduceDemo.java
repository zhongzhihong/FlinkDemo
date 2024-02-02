package com.zzh._04_aggregation;

import com.zzh.entity.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 归约聚合-reduce。
 * 对已有的数据进行归约处理，把每一个新输入的数据和当前已经归约出来的值，再做一个聚合计算。
 */
public class _03_ReduceDemo {
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
        keyedBy.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor ws1, WaterSensor ws2) {
                return new WaterSensor(ws1.getId(), ws2.getTs(), ws1.getVc() + ws2.getVc());
            }
        }).print();
        env.execute();
    }
}
