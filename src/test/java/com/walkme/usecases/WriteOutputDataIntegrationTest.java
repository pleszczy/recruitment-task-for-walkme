package com.walkme.usecases;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.walkme.entities.ActivityAccumulator;
import java.nio.file.Files;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
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
      assertEquals(1, files.size(), "Expected exactly one output file.");
    }
  }

  private static DataStreamSource<ActivityAccumulator> testData(StreamExecutionEnvironment env) {
    return env.fromElements(
        new ActivityAccumulator("2023-10-12", "user1", "testEnv", "activityType1", 1000L),
        new ActivityAccumulator("2023-10-12", "user1", "testEnv", "activityType3", 1000L),
        new ActivityAccumulator("2023-10-12", "user1", "testEnv2", "activityType2", 1000L),
        new ActivityAccumulator("2023-10-12", "user1", "testEnv2", "activityType4", 1000L),
        new ActivityAccumulator("2023-10-12", "user2", "testEnv", "activityType1", 1000L),
        new ActivityAccumulator("2023-10-12", "user2", "testEnv", "activityType3", 1000L),
        new ActivityAccumulator("2023-10-12", "user2", "testEnv2", "activityType2", 1000L),
        new ActivityAccumulator("2023-10-12", "user2", "testEnv3", "activityType4", 1000L)
    );
  }
}