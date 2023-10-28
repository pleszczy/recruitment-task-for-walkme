package com.walkme.usecases;

import com.walkme.generated.Activity;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.AvroParquetReaders;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadInputActivities {
  public static DataStreamSource<Activity> readInputData(Path dataPath, StreamExecutionEnvironment env) {
    var streamFormat = AvroParquetReaders.forSpecificRecord(Activity.class);
    var source = FileSource.forRecordStreamFormat(streamFormat, dataPath)
        .processStaticFileSet()
        .build();
    return env.fromSource(source, WatermarkStrategy.noWatermarks(), "activities data");
  }
}