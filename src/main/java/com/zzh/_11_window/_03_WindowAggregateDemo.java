package com.zzh._11_window;

import com.zzh.entity.WaterSensor;
import com.zzh.function.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class _03_WindowAggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> socketTextStream = env.socketTextStream("localhost", 7777).map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> waterSensorKS = socketTextStream.keyBy(WaterSensor::getId);
        // 窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> waterSensorWS = waterSensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 窗口函数
        /*
          第一个参数类型：输入数据的类型
          第二个参数类型：累加器的类型，存储的中间计算结果的类型
          第三个参数类型：输出的类型
         */
        waterSensorWS.aggregate(new AggregateFunction<WaterSensor, Integer, String>() {
            // 初始化累加器
            @Override
            public Integer createAccumulator() {
                return 0;
            }

            // 计算逻辑
            @Override
            public Integer add(WaterSensor waterSensor, Integer integer) {
                return integer + waterSensor.getVc();
            }

            // 获取结果
            @Override
            public String getResult(Integer integer) {
                return integer.toString();
            }

            // 一般不去用，会话窗口才会用到
            @Override
            public Integer merge(Integer integer, Integer acc1) {
                return null;
            }
        }).print();
        env.execute();
    }
}
