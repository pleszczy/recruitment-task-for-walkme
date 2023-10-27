package com.walkme;

import com.walkme.adapters.frameworks.jackson.ObjectMapperFactory;
import com.walkme.adapters.frameworks.okhttp3.HttpClientFactory;
import com.walkme.adapters.frameworks.resilence4j.ExponentialRetryFactory;
import com.walkme.adapters.repositories.EnvironmentRepository;
import java.time.Duration;

/**
 * TODO: Use guice
 */
public final class AppModule /*extends AbstractModule*/ {

  public static EnvironmentRepository environmentRepository() {
    var httpClient = HttpClientFactory.httpClient();
    var objectMapper = ObjectMapperFactory.objectMapper();
    var retry = ExponentialRetryFactory.exponentialRetry(Duration.ofSeconds(5), 2, 100);
    return new EnvironmentRepository(httpClient, objectMapper, retry);
  }

}