package com.walkme.usecases;

import com.walkme.entities.ActivityAccumulator;
import com.walkme.generated.DailyActivityAggregate;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

public class WriteOutputData {
  public static DataStreamSink<DailyActivityAggregate> execute(
      SingleOutputStreamOperator<ActivityAccumulator> aggregatedDataStream, Path outputPath) {
    return aggregatedDataStream
        .map(activityAcc -> new DailyActivityAggregate(activityAcc.date(), activityAcc.userId(),
            activityAcc.environment(), activityAcc.activityType(), activityAcc.runTime()))
        .keyBy(DailyActivityAggregate::getDate)
        .sinkTo(getFileSink(outputPath));
  }

  private static FileSink<DailyActivityAggregate> getFileSink(Path outputPath) {
    return FileSink
        .forBulkFormat(outputPath, AvroParquetWriters.forSpecificRecord(DailyActivityAggregate.class))
        .withBucketAssigner(new BucketAssigner<>() {
          @Override
          public String getBucketId(DailyActivityAggregate dailyActivityAggregates, Context context) {
            var date = dailyActivityAggregates.get("date").toString();
            var parts = date.split("-");
            var year = parts[0];
            var month = parts[1];
            var day = parts[2];
            return "year=%s/month=%s/day=%s".formatted(year, month, day);
          }

          @Override
          public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
          }
        })
        .withOutputFileConfig(OutputFileConfig.builder()
            .withPartSuffix(".snappy.parquet")
            .build())
        .build();
  }
}