package com.walkme.adapters.mappers;

import com.walkme.common.TimeConverter;
import com.walkme.generated.Activity;
import org.apache.flink.api.common.functions.MapFunction;

public class MapActivityDtoToActivityEntity implements MapFunction<Activity, com.walkme.entities.Activity> {
  @Override
  public com.walkme.entities.Activity map(Activity dto) {
    return new com.walkme.entities.Activity(
        dto.getEnvironment(),
        dto.getUserId(),
        dto.getOptionalActivityType().orElse("UNKNOWN"),
        dto.getStartTimestamp(),
        dto.getOptionalEndTimestamp().orElse(TimeConverter.toTimestampAtEndOfDay(dto.getStartTimestamp()))
    );
  }
}