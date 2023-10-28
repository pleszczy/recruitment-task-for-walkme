package com.walkme.usecases;

import com.walkme.adapters.frameworks.flink.ActivityTimeWatermarkStrategyFactory;
import com.walkme.entities.ActivityAccumulator;
import com.walkme.generated.Activity;
import java.util.Set;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public final class PrepareDailyActivityAggregates {

  public static SingleOutputStreamOperator<ActivityAccumulator> execute(
      Path dataPath, Set<String> excludeActivitiesTypes, StreamExecutionEnvironment env) {
    return ReadInputActivities.readInputData(dataPath, env)
        .assignTimestampsAndWatermarks(ActivityTimeWatermarkStrategyFactory.get())
        .map(new MapNullActivitiesTypeToUnknownType())
        .filter(new FilterOutExcludedActivityTypes(excludeActivitiesTypes))
        .filter(new FilterOutActivitiesInActiveTestEnvironment())
        .keyBy(groupByUserIdEnvironmentActivityType())
        .window(TumblingEventTimeWindows.of(Time.days(1)))
        .aggregate(new AggregateActivities());
  }

  private static KeySelector<Activity, Tuple3<String, String, String>> groupByUserIdEnvironmentActivityType() {
    return new KeySelector<>() {
      @Override
      public Tuple3<String, String, String> getKey(Activity it) {
        return new Tuple3<>(it.getUserId(), it.getEnvironment(), it.getActivityType());
      }
    };
  }

}