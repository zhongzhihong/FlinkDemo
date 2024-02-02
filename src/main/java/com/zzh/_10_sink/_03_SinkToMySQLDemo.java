package com.zzh._10_sink;

import com.zzh.function.WaterSensorMapFunction;
import com.zzh.entity.WaterSensor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * JDBCSink的4个参数:
 * 第一个参数： 执行的SQL
 * 第二个参数： 预编译SQL，对占位符填充值
 * 第三个参数： 执行选项 ---> 攒批、重试
 * 第四个参数： 连接选项 ---> url、用户名、密码
 */
public class _03_SinkToMySQLDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("localhost", 7777).map(new WaterSensorMapFunction());
        SinkFunction<WaterSensor> jdbcSink = JdbcSink.sink(
                "INSERT INTO ws VALUES(?,?,?)",
                new JdbcStatementBuilder<WaterSensor>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                        //每收到一条WaterSensor，如何去填充占位符
                        preparedStatement.setString(1, waterSensor.getId());
                        preparedStatement.setLong(2, waterSensor.getTs());
                        preparedStatement.setInt(3, waterSensor.getVc());
                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3) // 重试次数
                        .withBatchSize(100) // 批次的大小：条数
                        .withBatchIntervalMs(3000) // 批次的时间
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                        .withUsername("root")
                        .withPassword("123456zzh")
                        .withConnectionCheckTimeoutSeconds(60) // 重试的超时时间
                        .build()
        );
        // 这里只能用addSink
        sensorDS.addSink(jdbcSink);
        env.execute();
    }
}
