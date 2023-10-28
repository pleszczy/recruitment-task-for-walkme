package com.walkme.usecases;

import com.walkme.Activity;
import org.apache.flink.api.common.functions.MapFunction;

public class MapNullActivitiesTypeToUnknownType implements MapFunction<Activity, Activity> {
  @Override
  public Activity map(Activity activity) {
    if (activity.getActivityType() == null) {
      return new Activity(activity.getEnvironment(), activity.getUserId(), "UNKNOWN", activity.getStartTimestamp(),
          activity.getEndTimestamp());
    }
    return activity;
  }
}