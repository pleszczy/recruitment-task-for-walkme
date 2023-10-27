package com.walkme.adapters.repositories;

import static org.eclipse.jetty.http.HttpStatus.NOT_FOUND_404;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.walkme.entities.Environment;
import com.walkme.exceptions.FetchEnvironmentException;
import io.github.resilience4j.retry.Retry;
import java.io.IOException;
import java.util.Optional;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnvironmentRepository {
  private static final Logger LOG = LoggerFactory.getLogger(EnvironmentRepository.class);
  private static final String ENVIRONMENT_URL = "http://localhost:8080/testEnvironments/%s";

  private final OkHttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final Retry retry;

  public EnvironmentRepository(OkHttpClient httpClient, ObjectMapper objectMapper, Retry retry) {
    this.httpClient = httpClient;
    this.objectMapper = objectMapper;
    this.retry = retry;
  }

  /**
   * Fetches environment details for a given user ID.
   *
   * @param userId The user ID for which environment details are to be fetched.
   * @return The environment details.
   * @throws FetchEnvironmentException if there is an error fetching the environment details.
   */
  public Optional<Environment> fetchEnvironment(String userId) throws FetchEnvironmentException {
    var request = new Request.Builder().url(ENVIRONMENT_URL.formatted(userId)).build();

    try {
      return Retry.decorateCheckedSupplier(retry, () -> {
        try (var response = httpClient.newCall(request).execute();
             var body = response.body()) {

          if (is404(response)) {
            LOG.debug("Environment not found for user: {}", userId);
            return Optional.<Environment>empty();
          } else if (is4xx(response.code())) {
            LOG.error("Bad request received for user: {}, code: {}", userId, response.code());
            return Optional.<Environment>empty();
          }

          validateResponse(response, body);
          var environment = objectMapper.readValue(body.string(), Environment.class);
          return Optional.ofNullable(environment);
        }
      }).get();
    } catch (Throwable t) {
      preventCatchingJvmErrors(t);
      LOG.error("Error fetching environment for user {}", userId, t);
      throw new FetchEnvironmentException(userId, t);
    }
  }

  private boolean is404(Response response) {
    return response.code() == NOT_FOUND_404;
  }

  private boolean is4xx(int statusCode) {
    return (statusCode >= 400 && statusCode < 500);
  }

  private void validateResponse(Response response, ResponseBody body) throws IOException {
    if (!response.isSuccessful()) {
      LOG.warn("Received an unsuccessful HTTP response: {}", response.code());
      throw new IOException("Received an unsuccessful HTTP response: " + response.code());
    }

    if (body == null) {
      LOG.warn("Response body is null");
      throw new IOException("Response body is null");
    }
  }

  private void preventCatchingJvmErrors(Throwable t) throws Error {
    if (t instanceof Error) {
      LOG.error("Caught JVM error", t);
      throw (Error) t;
    }
  }
}