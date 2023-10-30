package com.walkme.usecases;

import com.walkme.AppModule;
import com.walkme.adapters.mappers.MapActivityDtoToActivityEntity;
import com.walkme.common.StringTuple3;
import com.walkme.entities.Activity;
import com.walkme.entities.ActivityAccumulator;
import java.util.Set;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
        .map(appModule.mapActivityDtoToActivityEntity())
        .filter(appModule.filterOutExcludedActivityTypesUseCase(excludeActivitiesTypes))
        .filter(appModule.filterOutActivitiesInActiveTestEnvironmentUseCase())
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Activity>forMonotonousTimestamps().withTimestampAssigner(
            ctx -> (activity, previousElementTimestamp) -> activity.startTimestamp()))
        .keyBy(it -> new StringTuple3(it.userId(), it.environment(), it.activityType()))
        .window(TumblingEventTimeWindows.of(Time.days(1)))
        .aggregate(appModule.aggregateActivitiesUseCase());
  }
}