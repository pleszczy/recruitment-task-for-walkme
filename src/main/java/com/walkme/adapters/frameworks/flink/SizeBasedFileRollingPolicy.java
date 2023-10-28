package com.walkme.adapters.frameworks.flink;

import java.io.IOException;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;

/**
 * This class represents a custom rolling policy that rolls over the file based on its size.
 *
 * <p>The policy rolls the file when its size exceeds a specified number of bytes.
 * It doesn't roll based on processing time or checkpoints.
 *
 * @param <IN>       The type of the input elements.
 * @param <BucketID> The type of the bucket identifier.
 */
public class SizeBasedFileRollingPolicy<IN, BucketID> extends CheckpointRollingPolicy<IN, BucketID> {

  private final long maxSizeBytes;

  /**
   * Initializes the rolling policy with the maximum size in bytes.
   *
   * @param maxSizeBytes The threshold size in bytes after which rolling should happen.
   */
  public SizeBasedFileRollingPolicy(long maxSizeBytes) {
    this.maxSizeBytes = maxSizeBytes;
  }

  @Override
  public boolean shouldRollOnCheckpoint(PartFileInfo<BucketID> partFileState) {
    // This policy does not roll on checkpoints.
    return false;
  }

  @Override
  public boolean shouldRollOnEvent(PartFileInfo<BucketID> partFileState, IN element) throws IOException {
    return partFileState.getSize() >= maxSizeBytes;
  }

  @Override
  public boolean shouldRollOnProcessingTime(PartFileInfo<BucketID> partFileState, long currentTime) {
    // This policy does not roll on processing time.
    return false;
  }
}