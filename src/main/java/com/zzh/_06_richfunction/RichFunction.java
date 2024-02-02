package com.zzh._06_richfunction;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * RichMapFunction：富函数
 * 其他函数与RichXXXFunction的区别：
 * 1、多了生命周期管理方法：
 *      open()：每个子任务在启动时调用一次
 *      close()：每个子任务在结束时调用一次
 * 2、多了运行时上下文：
 *      可以获取一些运行时信息。比如：子任务编号、子任务名称
 */
public class RichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5);

        source.map(new RichMapFunction<Integer, Integer>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("子任务编号为：" + getRuntimeContext().getIndexOfThisSubtask() +
                        "，子任务名称为：" + getRuntimeContext().getTaskNameWithSubtasks() +
                        " ====>调用open()方法");
            }

            @Override
            public Integer map(Integer integer) throws Exception {
                return integer + 1;
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("子任务编号为：" + getRuntimeContext().getIndexOfThisSubtask() +
                        "，子任务名称为：" + getRuntimeContext().getTaskNameWithSubtasks() +
                        " ====>调用close()方法");
            }
        }).print();

        env.execute();
    }
}
