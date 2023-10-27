package com.walkme.usecases;

import com.walkme.entities.ActivityAccumulator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.jetbrains.annotations.NotNull;

public class WriteDataUseCase {
  public static DataStreamSink<Tuple5<String, String, String, String, Long>> execute(
      SingleOutputStreamOperator<ActivityAccumulator> aggregatedDataStream, Path outputPath) {
    return aggregatedDataStream.map(mapToTuple())
        .sinkTo(getFileSink(outputPath));
  }

  @NotNull
  private static MapFunction<ActivityAccumulator, Tuple5<String, String, String, String, Long>> mapToTuple() {
    return new MapFunction<>() {
      @Override
      public Tuple5<String, String, String, String, Long> map(ActivityAccumulator it) {
        return new Tuple5<>(it.date(), it.userId(), it.environment(), it.activityType(), it.runTime());
      }
    };
  }

  private static FileSink<Tuple5<String, String, String, String, Long>> getFileSink(Path outputPath) {
    return FileSink
        .forRowFormat(outputPath,
            new SimpleStringEncoder<Tuple5<String, String, String, String, Long>>("UTF-8"))
        .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd"))
        .build();
  }
}