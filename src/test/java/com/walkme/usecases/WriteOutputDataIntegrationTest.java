package com.walkme.usecases;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.walkme.entities.ActivityAccumulator;
import com.walkme.generated.DailyActivityAggregate;
import java.nio.file.Files;
import java.util.ArrayList;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
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
        new ActivityAccumulator("2023-10-12", "user2", "testEnv2", "activityType2", 200L)
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

    try (var stream = Files.walk(tempDir.resolve("year=2023/month=10/day=12/"))) {
      var files = stream
          .filter(Files::isRegularFile)
          .toList();
      assertThat(files).hasSize(1);

      var hadoopPath = new org.apache.hadoop.fs.Path(files.get(0).toUri());
      var inputFile = HadoopInputFile.fromPath(hadoopPath, new Configuration());

      try (var reader = AvroParquetReader
          .<DailyActivityAggregate>builder(inputFile)
          .build()) {

        var records = new ArrayList<>();
        DailyActivityAggregate activityAggregate;
        while ((activityAggregate = reader.read()) != null) {
          records.add(activityAggregate);
        }

        assertThat(records).hasSize(2);
        assertThat(records).contains(new DailyActivityAggregate("2023-10-12", "user1", "testEnv", "activityType1", 100L));
        assertThat(records).contains(new DailyActivityAggregate("2023-10-12", "user2", "testEnv2", "activityType2", 200L));
      }
    }
  }
}