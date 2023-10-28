package com.walkme.common;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;

public class ParamParser {
  public static Set<String> excludeActivitiesTypes(String[] args) {
    var params = ParameterTool.fromArgs(args);
    return new HashSet<>(Arrays.asList(params.getRequired("excludedActivityTypes").split(",")));
  }

  public static Path inputPath(String[] args) {
    var params = ParameterTool.fromArgs(args);
    return new Path(params.get("inputPath", "data/"));
  }

  public static Path outputPath(String[] args) {
    var params = ParameterTool.fromArgs(args);
    return new Path(params.get("outputPath", "output/daily-activity"));
  }
}