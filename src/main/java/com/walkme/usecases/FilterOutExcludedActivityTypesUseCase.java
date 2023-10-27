package com.walkme.usecases;

import com.walkme.Activity;
import java.util.Set;
import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterOutExcludedActivityTypesUseCase implements FilterFunction<Activity> {
  private static final Logger LOG = LoggerFactory.getLogger(FilterOutExcludedActivityTypesUseCase.class);
  private final Set<String> excludedActivityTypes;

  public FilterOutExcludedActivityTypesUseCase(Set<String> excludedActivityTypes) {
    this.excludedActivityTypes = excludedActivityTypes;
  }

  @Override
  public boolean filter(Activity activity) {
    boolean isExcluded = excludedActivityTypes.contains(activity.getActivityType());
    if (isExcluded) {
      LOG.debug("Filtering out activity: {}. It's among the excluded activity types: {}", activity,
          excludedActivityTypes);
    }
    return !isExcluded;
  }
}