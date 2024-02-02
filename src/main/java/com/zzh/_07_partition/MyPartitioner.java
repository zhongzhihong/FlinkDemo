package com.zzh._07_partition;

import org.apache.flink.api.common.functions.Partitioner;

public class MyPartitioner implements Partitioner<String> {
    @Override
    public int partition(String s, int i) {
        return Integer.parseInt(s) % i;
    }
}
