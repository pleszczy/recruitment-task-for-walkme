package com.walkme.adapters.frameworks.flink;

import com.walkme.Activity;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

public class ActivityTimeWatermarkStrategyFactory {
  public static WatermarkStrategy<Activity> get() {
    TimestampAssignerSupplier<Activity> timestampAssignerSupplier =
        ctx -> (activity, previousElementTimestamp) -> activity.getStartTimestamp();
    return WatermarkStrategy.<Activity>forMonotonousTimestamps().withTimestampAssigner(timestampAssignerSupplier);
  }
}