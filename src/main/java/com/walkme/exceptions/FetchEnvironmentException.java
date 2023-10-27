package com.walkme.exceptions;

public class FetchEnvironmentException extends RuntimeException {
  public FetchEnvironmentException(String userId, Throwable cause) {
    super("Failed to fetch environment for %s".formatted(userId), cause);
  }
}