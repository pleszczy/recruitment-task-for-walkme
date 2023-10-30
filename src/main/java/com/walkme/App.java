package com.walkme;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
  private static final Logger log = LoggerFactory.getLogger(App.class);
  private static final AppModule appModule = new AppModule();

  public static void main(String[] args) throws Exception {
    var paramParser = appModule.paramParser(args);
    var inputPath = paramParser.inputPath();
    var outputPath = paramParser.outputPath();
    var excludeActivitiesTypes = paramParser.excludeActivitiesTypes();
    log.info("Executing App with inputPath={}, outputPath={}, excludeActivitiesTypes={}", inputPath, outputPath,
        excludeActivitiesTypes);
    var flinkJob = new FlinkJob(appModule);
    flinkJob.execute(inputPath, outputPath, excludeActivitiesTypes);
  }
}