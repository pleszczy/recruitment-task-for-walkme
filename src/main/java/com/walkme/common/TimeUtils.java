package com.walkme.common;

import static java.time.ZoneOffset.UTC;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class TimeUtils {
  public static long toTimestampAtStartOfDay(String date) {
    return LocalDate.parse(date).atStartOfDay(UTC).toInstant().toEpochMilli();
  }

  public static long toTimestampAtEndOfDay(String date) {
    return LocalDate.parse(date).atStartOfDay(UTC).plusDays(1).minusNanos(1).toInstant().toEpochMilli();
  }

  public static long toTimestampAtEndOfDay(long timestamp) {
    return Instant.ofEpochMilli(timestamp).atOffset(UTC).toLocalDate().atStartOfDay(UTC).plusDays(1).minusNanos(1)
        .toInstant().toEpochMilli();
  }

  public static String toUtcDate(long timestamp) {
    return DateTimeFormatter.ofPattern("yyyy-MM-dd")
        .withZone(ZoneOffset.UTC)
        .format(Instant.ofEpochMilli(timestamp));
  }
}