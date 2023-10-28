package com.walkme.usecases;

import com.walkme.adapters.frameworks.flink.ActivityTimeBucketAssigner;
import com.walkme.entities.ActivityAccumulator;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class WriteOutputDataUseCase {
  public static DataStreamSink<String> execute(
      SingleOutputStreamOperator<ActivityAccumulator> aggregatedDataStream, Path outputPath) {

    SingleOutputStreamOperator<String> csvDataStream = aggregatedDataStream
        // Repartition the stream by date to ensure only one file per day
        .keyBy(ActivityAccumulator::date)
        .map(it -> String.format("%s,%s,%s,%s,%d",
            it.date(), it.userId(), it.environment(), it.activityType(), it.runTime()))
        .returns(Types.STRING);
    return csvDataStream.sinkTo(getFileSink(outputPath));
  }

  private static FileSink<String> getFileSink(Path outputPath) {
    return FileSink
        .forRowFormat(outputPath, new SimpleStringEncoder<String>(StandardCharsets.UTF_8.name()))
        .withBucketAssigner(new ActivityTimeBucketAssigner())
        .build();
  }
}