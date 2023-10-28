package com.walkme.usecases;

import com.walkme.adapters.frameworks.flink.SizeBasedFileRollingPolicy;
import com.walkme.entities.ActivityAccumulator;
import com.walkme.generated.DailyActivityAggregate;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

public class WriteOutputData {
  public static DataStreamSink<DailyActivityAggregate> execute(
      SingleOutputStreamOperator<ActivityAccumulator> aggregatedDataStream, Path outputPath) {

    KeyedStream<DailyActivityAggregate, Tuple4<String, String, String, String>> avroDataStream = aggregatedDataStream
        .map(activityAcc -> new DailyActivityAggregate(activityAcc.date(), activityAcc.userId(),
            activityAcc.environment(), activityAcc.activityType(), activityAcc.runTime()))
        .setParallelism(1)
        .keyBy(new KeySelector<DailyActivityAggregate, Tuple4<String, String, String, String>>() {
          @Override
          public Tuple4<String, String, String, String> getKey(DailyActivityAggregate it) {
            return new Tuple4<>(it.getDate(), it.getUserId(), it.getActivityType(), it.getEnvironment());
          }
        });

    return avroDataStream
        .sinkTo(getFileSink(outputPath));
  }

  private static FileSink<DailyActivityAggregate> getFileSink(Path outputPath) {
    return FileSink
        .forBulkFormat(outputPath, AvroParquetWriters.forSpecificRecord(DailyActivityAggregate.class))
        .withBucketAssigner(new BucketAssigner<>() {
          @Override
          public String getBucketId(DailyActivityAggregate dailyActivityAggregates, Context context) {
            String date = dailyActivityAggregates.get("date").toString();
            String[] parts = date.split("-");
            String year = parts[0];
            String month = parts[1];
            String day = parts[2];
            return String.format("year=%s/month=%s/day=%s", year, month, day);
          }

          @Override
          public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
          }
        })
        .withRollingPolicy(new SizeBasedFileRollingPolicy<>(2L * 1024L * 1024L * 1024L))
        .withOutputFileConfig(OutputFileConfig.builder()
            .withPartSuffix(".snappy.parquet")
            .build())
        .build();
  }
}