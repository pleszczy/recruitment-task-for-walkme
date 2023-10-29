package com.walkme.adapters.frameworks.flink;

import com.walkme.generated.DailyActivityAggregate;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

public class ActivityTimeBucketAssigner implements BucketAssigner<DailyActivityAggregate, String> {
  @Override
  public String getBucketId(DailyActivityAggregate dailyActivityAggregates, Context context) {
    var date = dailyActivityAggregates.getDate();
    var parts = date.split("-");
    var year = parts[0];
    var month = parts[1];
    var day = parts[2];
    return "year=%s/month=%s/day=%s".formatted(year, month, day);
  }

  @Override
  public SimpleVersionedSerializer<String> getSerializer() {
    return SimpleVersionedStringSerializer.INSTANCE;
  }
}