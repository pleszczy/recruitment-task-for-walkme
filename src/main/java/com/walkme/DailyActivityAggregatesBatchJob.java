package com.walkme;

import com.walkme.adapters.frameworks.flink.ActivityTimeWatermarkStrategyFactory;
import com.walkme.entities.Activity;
import com.walkme.entities.ActivityAccumulator;
import com.walkme.usecases.AggregateActivities;
import com.walkme.usecases.FilterOutActivitiesInActiveTestEnvironment;
import com.walkme.usecases.FilterOutExcludedActivityTypes;
import com.walkme.adapters.mappers.MapActivityDtoToActivityEntity;
import com.walkme.usecases.ReadInputActivities;
import java.util.Set;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public final class DailyActivityAggregatesBatchJob {

  public static SingleOutputStreamOperator<ActivityAccumulator> execute(
      Path dataPath, Set<String> excludeActivitiesTypes, StreamExecutionEnvironment env) {
    return ReadInputActivities.readInputData(dataPath, env)
        .assignTimestampsAndWatermarks(ActivityTimeWatermarkStrategyFactory.get())
        .map(new MapActivityDtoToActivityEntity())
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
        return new Tuple3<>(it.userId(), it.environment(), it.activityType());
      }
    };
  }

}