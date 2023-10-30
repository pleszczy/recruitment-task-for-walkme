package com.walkme.usecases;

import com.walkme.generated.Activity;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.impl.StreamFormatAdapter;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.AvroParquetReaders;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadInputDataUseCase {
  public DataStreamSource<Activity> execute(Path dataPath, StreamExecutionEnvironment env) {
    var streamFormat = AvroParquetReaders.forSpecificRecord(Activity.class);
    var source = FileSource.forBulkFileFormat(toBulkFormat(streamFormat), dataPath)
        .processStaticFileSet()
        .build();
    return env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "activities data");
  }

  private BulkFormat<Activity, FileSourceSplit> toBulkFormat(
      StreamFormat<Activity> activityStreamFormat) {
    return new StreamFormatAdapter<>(activityStreamFormat);
  }
}