package com.walkme.usecases;

import com.walkme.common.TimeConverter;
import com.walkme.generated.Activity;
import org.apache.flink.api.common.functions.MapFunction;

public class MapOptionalFields implements MapFunction<Activity, Activity> {
  @Override
  public Activity map(Activity activity) {
    return Activity.newBuilder()
        .setEnvironment(activity.getEnvironment())
        .setUserId(activity.getUserId())
        .setActivityType(activity.getOptionalActivityType().orElse("UNKNOWN"))
        .setStartTimestamp(activity.getStartTimestamp())
        .setEndTimestamp(activity.getOptionalEndTimestamp()
            .orElse(TimeConverter.toTimestampAtEndOfDay(activity.getStartTimestamp())))
        .build();
  }
}