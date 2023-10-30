package com.walkme.usecases;

import com.walkme.adapters.frameworks.flink.ActivityTimeBucketAssigner;
import com.walkme.entities.ActivityAccumulator;
import com.walkme.generated.DailyActivityAggregate;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;

public class WriteOutputDataUseCase {
  public DataStreamSink<DailyActivityAggregate> execute(
      SingleOutputStreamOperator<ActivityAccumulator> aggregatedDataStream, Path outputPath) {
    KeyedStream<DailyActivityAggregate, String> stream = aggregatedDataStream
        .map(activityAcc -> DailyActivityAggregate.newBuilder()
            .setDate(activityAcc.date())
            .setUserId(activityAcc.userId())
            .setEnvironment(activityAcc.environment())
            .setActivityType(activityAcc.activityType())
            .setRunTimeMs(activityAcc.runTime())
            .build())
        .keyBy(DailyActivityAggregate::getDate);
    // Debugging writing to parquet issues
    stream.print();
    return stream
        .sinkTo(getFileSink(outputPath));
  }

  private FileSink<DailyActivityAggregate> getFileSink(Path outputPath) {
    return FileSink
        .forBulkFormat(outputPath, AvroParquetWriters.forSpecificRecord(DailyActivityAggregate.class))
        .withBucketAssigner(new ActivityTimeBucketAssigner())
        .withOutputFileConfig(outputFileConfig())
        .build();
  }

  private OutputFileConfig outputFileConfig() {
    return OutputFileConfig.builder()
        .withPartSuffix(".snappy.parquet")
        .build();
  }
}