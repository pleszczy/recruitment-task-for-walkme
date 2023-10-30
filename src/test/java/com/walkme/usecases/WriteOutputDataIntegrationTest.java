package com.walkme.usecases;

import static org.assertj.core.api.Assertions.assertThat;

import com.walkme.entities.ActivityAccumulator;
import com.walkme.generated.DailyActivityAggregate;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(MiniClusterExtension.class)
class WriteOutputDataIntegrationTest {
  @RegisterExtension
  public static final MiniClusterExtension MINI_CLUSTER_RESOURCE = new MiniClusterExtension(
      new MiniClusterResourceConfiguration.Builder()
          .setNumberSlotsPerTaskManager(2)
          .setNumberTaskManagers(1)
          .build());

  @TempDir
  private java.nio.file.Path tempDir;

  private WriteOutputData sut;

  private static DataStreamSource<ActivityAccumulator> testData(StreamExecutionEnvironment env) {
    return env.fromElements(
        new ActivityAccumulator("2023-10-12", "user1", "testEnv", "activityType1", 100L),
        new ActivityAccumulator("2023-10-12", "user2", "testEnv2", "activityType2", 200L),
        new ActivityAccumulator("2023-10-12", "user3", "testEnv3", "activityType3", 300L)
    );
  }

  @BeforeEach
  void beforeEach() {
    sut = new WriteOutputData();
  }

  @Test
  void should_correctly_write_output_files() throws Exception {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();

    sut.execute(testData(env), new Path(tempDir.toUri()));
    env.execute("should_correctly_write_output_files");

    var dailyActivityAggregates = readOutputFileContent();
    assertThat(dailyActivityAggregates).hasSize(3);
    assertThat(dailyActivityAggregates).contains(
        new DailyActivityAggregate("2023-10-12", "user1", "testEnv", "activityType1", 100L));
    assertThat(dailyActivityAggregates).contains(
        new DailyActivityAggregate("2023-10-12", "user2", "testEnv2", "activityType2", 200L));
    assertThat(dailyActivityAggregates).contains(
        new DailyActivityAggregate("2023-10-12", "user3", "testEnv3", "activityType3", 300L));
  }

  private List<DailyActivityAggregate> readOutputFileContent() throws IOException {
    try (var fileStream = Files.walk(tempDir.resolve("year=2023/month=10/day=12"))) {
      return fileStream.filter(Files::isRegularFile)
          .findFirst()
          .map(this::readParquetFile)
          .orElseThrow(() -> new IOException("No output file found"));
    }
  }

  private List<DailyActivityAggregate> readParquetFile(java.nio.file.Path filePath) throws UncheckedIOException {
    try (var reader = AvroParquetReader.<DailyActivityAggregate>builder(
        HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(filePath.toUri()), new Configuration())).build()) {
      return readerToStream(reader)
          .takeWhile(Objects::nonNull)
          .toList();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read parquet file", e);
    }
  }

  @NotNull
  private Stream<DailyActivityAggregate> readerToStream(ParquetReader<DailyActivityAggregate> reader) {
    return Stream.generate(() -> {
      try {
        return reader.read();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });
  }
}