package com.walkme.adapters.frameworks.flink;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

public class ActivityTimeBucketAssigner implements BucketAssigner<String, String> {
  @Override
  public String getBucketId(String line, Context context) {
    return line.split(",")[0];
  }

  @Override
  public SimpleVersionedSerializer<String> getSerializer() {
    return SimpleVersionedStringSerializer.INSTANCE;
  }
}