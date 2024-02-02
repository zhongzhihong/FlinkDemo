package com.zzh._11_window;

import com.zzh.function.WaterSensorMapFunction;
import com.zzh.entity.WaterSensor;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class _01_WindowApiDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("localhost", 7777).map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> waterSensorKS = waterSensorDS.keyBy(waterSensor -> waterSensor.getId());

        // 窗口分配器
        // 1、基于时间的
        // 滚动窗口：窗口长度10s
        waterSensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 滑动窗口：窗口长度10s，滑动步长2s
        waterSensorKS.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2)));
        // 会话窗口：间隔10s没有数据来，则将前面的数据划分为一个窗口
        waterSensorKS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));

        // 2、基于计数的
        // 滚动窗口：窗口长度5个元素
        waterSensorKS.countWindow(5);
        // 滑动窗口：窗口长度5个元素，窗口步长2个元素
        waterSensorKS.countWindow(5, 2);
        // 全局窗口
        waterSensorKS.window(GlobalWindows.create());

        // 窗口函数
        WindowedStream<WaterSensor, String, TimeWindow> waterSensorWS = waterSensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 1、增量聚合：来一条数据计算一条数据，窗口触发的时候输出计算结果
        // waterSensorWS.reduce();
        // waterSensorWS.aggregate();

        // 2、全窗口函数：数据来了不计算，存起来，窗口触发的时候计算并输出结果
        // waterSensorWS.process();

        env.execute();
    }
}
