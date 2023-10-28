package com.walkme;

import static org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.createLocalEnvironmentWithWebUI;

import com.walkme.common.ParamParser;
import com.walkme.generated.Activity;
import com.walkme.generated.DailyActivityAggregate;
import com.walkme.usecases.WriteOutputData;
import java.util.Set;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.utils.AvroKryoSerializerUtils.AvroSchemaSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.jetbrains.annotations.NotNull;

public class App {
  public static void main(String[] args) throws Exception {
    executeJob(ParamParser.inputPath(args), ParamParser.outputPath(args), ParamParser.excludeActivitiesTypes(args));
  }

  public static void executeJob(Path inputPath, Path outputPath, Set<String> excludeActivitiesTypes) throws Exception {
    try (var env = setupEnvironment()) {
      var dailyAggregatesStream = DailyActivityAggregatesBatchJob.execute(inputPath, excludeActivitiesTypes, env);
      WriteOutputData.execute(dailyAggregatesStream, outputPath);
      env.execute("Walkme Take Home Assignment");
    }
  }

  @NotNull
  private static StreamExecutionEnvironment setupEnvironment() {
    var env = createLocalEnvironmentWithWebUI(new Configuration());
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
    env.setRestartStrategy(RestartStrategies.fallBackRestart());
    env.registerTypeWithKryoSerializer(Activity.class, AvroSchemaSerializer.class);
    env.registerTypeWithKryoSerializer(DailyActivityAggregate.class, AvroSchemaSerializer.class);
    return env;
  }
}