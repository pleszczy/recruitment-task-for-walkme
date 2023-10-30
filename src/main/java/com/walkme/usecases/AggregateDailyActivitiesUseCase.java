package com.walkme.usecases;

import com.walkme.AppModule;
import com.walkme.adapters.mappers.MapActivityDtoToActivityEntity;
import com.walkme.entities.Activity;
import com.walkme.entities.ActivityAccumulator;
import java.util.Set;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public final class AggregateDailyActivitiesUseCase {
  private final AppModule appModule;

  public AggregateDailyActivitiesUseCase(AppModule appModule) {
    this.appModule = appModule;
  }

  public SingleOutputStreamOperator<ActivityAccumulator> execute(
      DataStreamSource<com.walkme.generated.Activity> inputStream, Set<String> excludeActivitiesTypes) {
    return inputStream
        .map(new MapActivityDtoToActivityEntity())
        .filter(new FilterOutExcludedActivityTypesUseCase(excludeActivitiesTypes))
        .filter(new FilterOutActivitiesInActiveTestEnvironmentUseCase(appModule))
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.<Activity>forMonotonousTimestamps().withTimestampAssigner(
                ctx -> (activity, previousElementTimestamp) -> activity.startTimestamp()))
        .keyBy(new KeySelector<Activity, Tuple3<String, String, String>>() {
          @Override
          public Tuple3<String, String, String> getKey(Activity it) {
            return new Tuple3<>(it.userId(), it.environment(), it.activityType());
          }
        })
        .window(TumblingEventTimeWindows.of(Time.days(1)))
        .aggregate(new AggregateActivitiesUseCase());
  }
}