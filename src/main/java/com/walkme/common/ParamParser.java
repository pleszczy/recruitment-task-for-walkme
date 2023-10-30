package com.walkme.common;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;

public class ParamParser {
  private final ParameterTool params;

  public ParamParser(String[] args) {
    params = ParameterTool.fromArgs(args);
  }

  public Set<String> excludeActivitiesTypes() {
    return Optional.ofNullable(params.get("excludedActivityTypes"))
        .map(it -> {
          String[] excludedActivityTypes = it.split(",");
          return new HashSet<>(Arrays.asList(excludedActivityTypes));
        })
        .orElse(new HashSet<>());
  }

  public Path inputPath() {
    return new Path(params.get("inputPath", "data/"));
  }

  public Path outputPath() {
    return new Path(params.get("outputPath", "output/daily-activity"));
  }
}