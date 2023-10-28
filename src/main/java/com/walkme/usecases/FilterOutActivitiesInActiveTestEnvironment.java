package com.walkme.usecases;

import static com.walkme.common.TimeUtils.toTimestampAtEndOfDay;
import static com.walkme.common.TimeUtils.toTimestampAtStartOfDay;
import static com.walkme.common.TimeUtils.toUtcDate;

import com.walkme.AppModule;
import com.walkme.adapters.repositories.EnvironmentRepository;
import com.walkme.entities.Environment;
import com.walkme.generated.Activity;
import java.util.Objects;
import java.util.Optional;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterOutActivitiesInActiveTestEnvironment extends RichFilterFunction<Activity> {
  private static final Logger LOG = LoggerFactory.getLogger(FilterOutActivitiesInActiveTestEnvironment.class);
  private transient EnvironmentRepository environmentRepository;

  @Override
  public void open(Configuration parameters) {
    this.environmentRepository = AppModule.environmentRepository();
  }

  @Override
  public boolean filter(Activity activity) {
    return environmentRepository.fetchEnvironment(activity.getUserId())
        .map(env -> {
          boolean isExcluded = activityOccursInActiveTestEnvironment(activity, env);
          if (isExcluded) {
            logExcludedActivity(activity, env);
          }
          return !isExcluded;
        }).orElse(true);
  }

  /**
   * Determines if an activity occurs during the active test environment time.
   */
  private boolean activityOccursInActiveTestEnvironment(Activity activity, Environment env) {
    boolean isSameTestEnvironment = Objects.equals(activity.getEnvironment(), env.environment());
    return isSameTestEnvironment && isWithinTestEnvironmentActiveTime(activity, env);
  }

  /**
   * Checks if the activity's time range overlaps with the active test environment time.
   */
  private boolean isWithinTestEnvironmentActiveTime(Activity activity, Environment environment) {
    long activeFrom = toTimestampAtStartOfDay(environment.activeFrom());
    long activeUntil = toTimestampAtEndOfDay(environment.activeUntil());
    long activityStart = activity.getStartTimestamp();
    long activityEnd = getActivityEndTimestamp(activity);

    return (activityStart >= activeFrom && activityStart <= activeUntil)
        || (activityEnd >= activeFrom && activityEnd <= activeUntil);
  }

  /**
   * Retrieves the end timestamp of the activity or defaults to the end of the date of the start timestamp.
   */
  private long getActivityEndTimestamp(Activity activity) {
    return Optional.ofNullable(activity.getEndTimestamp())
        .orElse(toTimestampAtEndOfDay(activity.getStartTimestamp()));
  }

  private void logExcludedActivity(Activity activity, Environment environment) {
    String startISO = toUtcDate(activity.getStartTimestamp());
    String endISO = toUtcDate(getActivityEndTimestamp(activity));
    LOG.debug("Excluding activity: [userID: {}, env: {}, start: {}, end: {}] due to active test environment: {}",
        activity.getUserId(), activity.getEnvironment(), startISO, endISO, environment);
  }
}