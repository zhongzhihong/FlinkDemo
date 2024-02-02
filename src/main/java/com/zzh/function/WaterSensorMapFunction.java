package com.zzh.function;

import com.zzh.entity.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

public class WaterSensorMapFunction implements MapFunction<String, WaterSensor> {
    @Override
    public WaterSensor map(String value) {
        String[] datas = value.split(",");
        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
    }
}
