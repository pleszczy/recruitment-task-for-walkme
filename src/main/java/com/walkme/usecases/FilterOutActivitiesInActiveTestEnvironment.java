package com.walkme.usecases;

import com.walkme.AppModule;
import com.walkme.adapters.repositories.EnvironmentRepository;
import com.walkme.common.TimeConverter;
import com.walkme.entities.Activity;
import com.walkme.entities.Environment;
import java.util.Objects;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterOutActivitiesInActiveTestEnvironment extends RichFilterFunction<Activity> {
  private static final Logger LOG = LoggerFactory.getLogger(FilterOutActivitiesInActiveTestEnvironment.class);
  private transient EnvironmentRepository environmentRepository;

  @Override
  public void open(Configuration parameters) {
    var appModule = new AppModule();
    this.environmentRepository = appModule.environmentRepository();
  }

  @Override
  public boolean filter(Activity activity) {
    return environmentRepository.fetchEnvironment(activity.userId())
        .map(env -> {
          boolean isExcluded = doesActivityOccurInActiveTestEnvironment(activity, env);
          if (isExcluded) {
            logExcludedActivity(activity, env);
          }
          return !isExcluded;
        }).orElse(true);
  }

  /**
   * Determines if an activity occurs during the active test environment time.
   */
  private boolean doesActivityOccurInActiveTestEnvironment(Activity activity, Environment env) {
    var isSameTestEnvironment = Objects.equals(activity.environment(), env.environment());
    return isSameTestEnvironment && isWithinTestEnvironmentActiveTime(activity, env);
  }

  /**
   * Checks if the activity's time range overlaps with the active test environment time.
   */
  private boolean isWithinTestEnvironmentActiveTime(Activity activity, Environment environment) {
    var activeFrom = TimeConverter.toTimestampAtStartOfDay(environment.activeFrom());
    var activeUntil = TimeConverter.toTimestampAtEndOfDay(environment.activeUntil());
    var activityStart = activity.startTimestamp();
    var activityEnd = activity.endTimestamp();

    return (activityStart >= activeFrom && activityStart <= activeUntil)
        || (activityEnd >= activeFrom && activityEnd <= activeUntil);
  }

  private void logExcludedActivity(Activity activity, Environment environment) {
    var startISO = TimeConverter.toUtcDate(activity.startTimestamp());
    var endISO = TimeConverter.toUtcDate(activity.endTimestamp());
    LOG.debug("Excluding activity: [userID: {}, env: {}, start: {}, end: {}] due to active test environment: {}",
        activity.userId(), activity.environment(), startISO, endISO, environment);
  }
}