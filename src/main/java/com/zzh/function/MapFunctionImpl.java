package com.zzh.function;

import com.zzh.entity.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

public class MapFunctionImpl implements MapFunction<WaterSensor, String> {
    @Override
    public String map(WaterSensor waterSensor) throws Exception {
        return waterSensor.getId();
    }
}
