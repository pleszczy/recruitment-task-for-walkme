package com.walkme.usecases;

import com.walkme.generated.Activity;
import java.util.Set;
import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterOutExcludedActivityTypes implements FilterFunction<Activity> {
  private static final Logger LOG = LoggerFactory.getLogger(FilterOutExcludedActivityTypes.class);
  private final Set<String> excludedActivityTypes;

  public FilterOutExcludedActivityTypes(Set<String> excludedActivityTypes) {
    this.excludedActivityTypes = excludedActivityTypes;
  }

  @Override
  public boolean filter(Activity activity) {
    var isExcluded = excludedActivityTypes.contains(activity.getActivityType());
    if (isExcluded) {
      LOG.debug("Filtering out activity: {}. It's among the excluded activity types: {}", activity,
          excludedActivityTypes);
    }
    return !isExcluded;
  }
}