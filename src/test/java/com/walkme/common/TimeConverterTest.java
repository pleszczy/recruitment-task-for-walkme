package com.walkme.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class TimeConverterTest {
  @Test
  void testToTimestampAtStartOfDay() {
    var date = "2023-10-26";
    var expected = 1698278400000L; // This is the UTC start of the date

    var actual = TimeConverter.toTimestampAtStartOfDay(date);

    assertEquals(expected, actual);
  }

  @Test
  void testToTimestampAtEndOfDay() {
    var date = "2023-10-26";
    var expected = 1698364799999L; // This is just one nanosecond before the UTC start of the next date

    var actual = TimeConverter.toTimestampAtEndOfDay(date);

    assertEquals(expected, actual);
  }

  @Test
  void testToTimestampAtEndOfDayWithTimestamp() {
    var timestamp = 1680120000000L; // Mar 29 2023 20:00:00
    var expected = 1680134399999L; // Just one nanosecond before the UTC start of the next date

    var actual = TimeConverter.toTimestampAtEndOfDay(timestamp);

    assertEquals(expected, actual);
  }

  @Test
  void testToISOFormat() {
    var timestamp = 1680120000000L;
    var expected = "2023-03-29";

    var actual = TimeConverter.toUtcDate(timestamp);

    assertEquals(expected, actual);
  }
}