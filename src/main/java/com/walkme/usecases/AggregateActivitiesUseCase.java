package com.walkme.usecases;

import static com.walkme.common.TimeUtils.toTimestampAtEndOfDay;
import static com.walkme.common.TimeUtils.toUtcDate;

import com.walkme.Activity;
import com.walkme.entities.ActivityAccumulator;
import java.util.Optional;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Aggregate daily activities to compute total runtime for each type of activity per user.
 */
public class AggregateActivitiesUseCase
    implements AggregateFunction<Activity, ActivityAccumulator, ActivityAccumulator> {

  /**
   * Calculate the runtime of an activity. If the activity doesn't have an end timestamp,
   * we use the end of the date as a default.
   */
  private long calculateActivityRunTime(Activity activity) {
    var endTimestamp = Optional.ofNullable(activity.getEndTimestamp())
        .orElse(toTimestampAtEndOfDay(activity.getStartTimestamp()));
    return endTimestamp - activity.getStartTimestamp();
  }

  @Override
  public ActivityAccumulator createAccumulator() {
    return
        new ActivityAccumulator("", "", "", "", 0L);
  }

  @Override
  public ActivityAccumulator add(Activity activity, ActivityAccumulator acc) {
    var timeMs = calculateActivityRunTime(activity);
    return new ActivityAccumulator(
        toUtcDate(activity.getStartTimestamp()),
        activity.getUserId(),
        activity.getEnvironment(),
        activity.getActivityType(),
        acc.runTime() + timeMs
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