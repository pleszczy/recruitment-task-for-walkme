package com.walkme.adapters.frameworks.okhttp3;

import java.time.Duration;
import okhttp3.OkHttpClient;

/**
 * Factory class responsible for creating OkHttpClient instances with pre-configured timeouts.
 *
 * <p>
 * TODO: It is recommended to create a YML configuration for the application to manage
 *  the timeout values externally rather than hardcoding them in the factory.
 * </p>
 */
public final class HttpClientFactory {
  public static final Duration CONNECTION_TIMEOUT = Duration.ofSeconds(3);
  public static final Duration READ_TIMEOUT = Duration.ofSeconds(10);
  public static final Duration WRITE_TIMEOUT = Duration.ofSeconds(10);

  public static OkHttpClient httpClient() {
    return new OkHttpClient.Builder()
        .connectTimeout(CONNECTION_TIMEOUT)
        .readTimeout(READ_TIMEOUT)
        .writeTimeout(WRITE_TIMEOUT)
        // FIXME: Cant access the following methods. I assume it's Jigsaw doing
        // TODO: Prevent OkHttpClient from retrying. Relay on resilence4j Retry instead
        // TODO: Use a small cache
        // .retryOnConnectionFailure(false)
        // .cache(new Cache(new File("tmp),10*1024*1024))
        .build();

  }
}