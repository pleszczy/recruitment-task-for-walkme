package com.walkme;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.walkme.adapters.frameworks.jackson.ObjectMapperFactory;
import com.walkme.adapters.frameworks.okhttp3.HttpClientFactory;
import com.walkme.adapters.frameworks.resilence4j.ExponentialRetryFactory;
import com.walkme.adapters.repositories.EnvironmentRepository;
import com.walkme.common.ParamParser;
import io.github.resilience4j.retry.Retry;
import java.time.Duration;
import okhttp3.OkHttpClient;

public final class AppModule {
  public EnvironmentRepository environmentRepository() {
    return new EnvironmentRepository(httpClient(), objectMapper(), exponentialRetry(Duration.ofSeconds(5), 2, 100));
  }

  public ObjectMapper objectMapper() {
    return ObjectMapperFactory.objectMapper();
  }

  public OkHttpClient httpClient() {
    return HttpClientFactory.httpClient();
  }

  public Retry exponentialRetry(Duration initialInterval, int multiplier, int maxRetries) {
    return ExponentialRetryFactory.exponentialRetry(initialInterval, multiplier, maxRetries);
  }

  public ParamParser paramParser(String[] args) {
    return new ParamParser(args);
  }
}