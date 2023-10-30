package com.walkme.usecases;

import static com.walkme.common.TimeConverter.toUtcDate;

import com.walkme.entities.Activity;
import com.walkme.entities.ActivityAccumulator;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Aggregate daily activities to compute total runtime for each type of activity per user.
 */
public class AggregateActivitiesUseCase
    implements AggregateFunction<Activity, ActivityAccumulator, ActivityAccumulator> {

  @Override
  public ActivityAccumulator createAccumulator() {
    return
        new ActivityAccumulator("", "", "", "", 0L);
  }

  @Override
  public ActivityAccumulator add(Activity activity, ActivityAccumulator acc) {
    var runTimeMs = activity.endTimestamp() - activity.startTimestamp();
    return new ActivityAccumulator(
        toUtcDate(activity.startTimestamp()),
        activity.userId(),
        activity.environment(),
        activity.activityType(),
        acc.runTime() + runTimeMs
    );
  }

  @Override
  public ActivityAccumulator getResult(ActivityAccumulator acc) {
    return acc;
  }

  @Override
  public ActivityAccumulator merge(ActivityAccumulator first, ActivityAccumulator second) {
    return new ActivityAccumulator(
        second.date(),
        second.userId(),
        second.environment(),
        second.activityType(),
        first.runTime() + second.runTime()
    );
  }
}