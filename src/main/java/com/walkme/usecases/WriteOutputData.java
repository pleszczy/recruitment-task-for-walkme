package com.walkme.usecases;

import com.walkme.entities.ActivityAccumulator;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

public class WriteOutputData {
  public static DataStreamSink<String> execute(
      SingleOutputStreamOperator<ActivityAccumulator> aggregatedDataStream, Path outputPath) {
    SingleOutputStreamOperator<String> csvDataStream = aggregatedDataStream
        .keyBy(ActivityAccumulator::date)
        .map(it -> String.format("%s,%s,%s,%s,%d",
            it.date(), it.userId(), it.environment(), it.activityType(), it.runTime()))
        .returns(Types.STRING);
    return csvDataStream.sinkTo(getFileSink(outputPath));
  }

  private static FileSink<String> getFileSink(Path outputPath) {
    return FileSink
        .forRowFormat(outputPath, new SimpleStringEncoder<String>(StandardCharsets.UTF_8.name()))
        .withBucketAssigner(new BucketAssigner<>() {
          @Override
          public String getBucketId(String line, Context context) {
            var date = line.split(",")[0];
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
        .build();
  }
}