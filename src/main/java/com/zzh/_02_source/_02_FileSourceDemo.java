package com.zzh._02_source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从文件中读取数据。【 `_02_WordCountDataStreamApi` 代码所采用的 `readTextFile()` 已经废弃】
 */
public class _02_FileSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 需要引入依赖：flink-connector-files
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("input/text.txt")).build();
        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "fileSource").print();
        env.execute();
    }
}
