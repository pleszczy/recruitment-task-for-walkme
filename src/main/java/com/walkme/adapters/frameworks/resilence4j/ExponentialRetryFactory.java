package com.walkme.adapters.frameworks.resilence4j;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.time.Duration;

public final class ExponentialRetryFactory {
  public static Retry exponentialRetry(Duration initialInterval, double multiplier, int maxRetries) {
    var debugFriendlyName =
        "exponential-retry-%dms-%.2f-%d".formatted(initialInterval.toMillis(), multiplier, maxRetries);
    return Retry.of(debugFriendlyName,
        RetryConfig.custom()
            .maxAttempts(maxRetries)
            .intervalFunction(IntervalFunction.ofExponentialBackoff(initialInterval.toMillis(), multiplier))
            .build());
  }
}