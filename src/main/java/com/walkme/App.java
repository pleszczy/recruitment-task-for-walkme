package com.walkme;

import static org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.createLocalEnvironmentWithWebUI;

import com.walkme.usecases.AggregateDailyActivitiesUseCase;
import com.walkme.usecases.ReadInputDataUseCase;
import com.walkme.usecases.WriteOutputDataUseCase;
import java.util.Set;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.jetbrains.annotations.NotNull;

public class App {
  private static final AppModule appModule = new AppModule();

  public static void main(String[] args) throws Exception {
    var paramParser = appModule.paramParser(args);
    executeJob(paramParser.inputPath(), paramParser.outputPath(), paramParser.excludeActivitiesTypes());
  }

  public static void executeJob(Path inputPath, Path outputPath, Set<String> excludeActivitiesTypes) throws Exception {
    try (var env = setupEnvironment()) {
      var inputDataStream = new ReadInputDataUseCase().execute(inputPath, env);
      // Debugging writing to parquet issues
      inputDataStream.print();
      var dailyAggregatedActivitiesStream = new AggregateDailyActivitiesUseCase(appModule)
          .execute(inputDataStream, excludeActivitiesTypes);
      // Debugging writing to parquet issues
      dailyAggregatedActivitiesStream.print();
      new WriteOutputDataUseCase().execute(dailyAggregatedActivitiesStream, outputPath);
      env.execute("Walkme Take Home Assignment");
    }
  }

  @NotNull
  private static StreamExecutionEnvironment setupEnvironment() {
    var env = createLocalEnvironmentWithWebUI(new Configuration());
    env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    env.setRestartStrategy(RestartStrategies.fallBackRestart());
    return env;
  }
}