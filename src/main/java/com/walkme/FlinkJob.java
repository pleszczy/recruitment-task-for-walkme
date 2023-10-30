package com.walkme;

import com.walkme.usecases.AggregateDailyActivitiesUseCase;
import com.walkme.usecases.ReadInputDataUseCase;
import com.walkme.usecases.WriteOutputDataUseCase;
import java.util.Set;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkJob {
  private final AppModule appModule;

  public FlinkJob(AppModule appModule) {
    this.appModule = appModule;
  }

  public void execute(Path inputPath, Path outputPath, Set<String> excludeActivitiesTypes) throws Exception {
    try (var env = setupEnvironment()) {
      var inputDataStream = new ReadInputDataUseCase().execute(inputPath, env);
      var aggregatedStream = new AggregateDailyActivitiesUseCase(appModule)
          .execute(inputDataStream, excludeActivitiesTypes);
      new WriteOutputDataUseCase().execute(aggregatedStream, outputPath);
      env.execute();
    }
  }

  private StreamExecutionEnvironment setupEnvironment() {
    return StreamExecutionEnvironment.getExecutionEnvironment()
        .setRuntimeMode(RuntimeExecutionMode.BATCH);
  }
}