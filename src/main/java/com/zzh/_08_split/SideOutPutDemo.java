package com.zzh._08_split;

import com.zzh.entity.WaterSensor;
import com.zzh.function.WaterSensorMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 分流（二）
 */
public class SideOutPutDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("localhost", 7777).map(new WaterSensorMapFunction());

        OutputTag<WaterSensor> s1Tag = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2Tag = new OutputTag<>("s2", Types.POJO(WaterSensor.class));

        SingleOutputStreamOperator<WaterSensor> process = waterSensorDS.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor waterSensor, ProcessFunction<WaterSensor, WaterSensor>.Context context, Collector<WaterSensor> collector) {
                if ("s1".equals(waterSensor.getId())) {
                    // 如果是 s1 ，则放到侧输出流 s1 中
                    context.output(s1Tag, waterSensor);
                } else if ("s2".equals(waterSensor.getId())) {
                    // 如果是 s2 ，则放到侧输出流 s2 中
                    context.output(s2Tag, waterSensor);
                } else {
                    collector.collect(waterSensor);
                }
            }
        });
        // 打印主流
        process.print("主流");
        // 打印侧输出流
        process.getSideOutput(s1Tag).print("s1");
        process.getSideOutput(s2Tag).print("s2");
        env.execute();
    }
}
