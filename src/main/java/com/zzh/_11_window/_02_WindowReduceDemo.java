package com.zzh._11_window;

import com.zzh.function.WaterSensorMapFunction;
import com.zzh.entity.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class _02_WindowReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> socketTextStream = env.socketTextStream("localhost", 7777).map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> socketKS = socketTextStream.keyBy(WaterSensor::getId);
        // 窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> socketWS = socketKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 窗口函数：增量聚合reduce
        socketWS.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor ws1, WaterSensor ws2) throws Exception {
                return new WaterSensor(ws1.getId(), ws2.getTs(), ws1.getVc() + ws2.getVc());
            }
        }).print();
        env.execute();
    }
}
