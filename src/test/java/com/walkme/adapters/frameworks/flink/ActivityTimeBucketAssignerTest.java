package com.walkme.adapters.frameworks.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.walkme.generated.DailyActivityAggregate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActivityTimeBucketAssignerTest {
  private ActivityTimeBucketAssigner sut;

  @BeforeEach
  public void beforeEach() {
    sut = new ActivityTimeBucketAssigner();
  }

  @Test
  void should_create_correct_buckets() {
    var aggregate = new DailyActivityAggregate();
    aggregate.setDate("2023-10-12");

    String bucketId = sut.getBucketId(aggregate, null);

    assertEquals("year=2023/month=10/day=12", bucketId, "Bucket ID should match the expected pattern.");
  }

}