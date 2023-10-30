package com.walkme.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class TimeConverterTest {
  @Test
  void should_convert_date_to_timestamp_at_start_of_day() {
    var date = "2023-10-26";
    var expected = 1698278400000L; // start of the date

    var actual = TimeConverter.toTimestampAtStartOfDay(date);

    assertEquals(expected, actual);
  }

  @Test
  void should_convert_date_to_timestamp_at_end_of_day() {
    var date = "2023-10-26";
    var expected = 1698364799999L; // one nanosecond before the start of the next date

    var actual = TimeConverter.toTimestampAtEndOfDay(date);

    assertEquals(expected, actual);
  }

  @Test
  void should_convert_timestamp_to_timestamp_at_end_of_day_with_timestamp_as_input() {
    var timestamp = 1680120000000L; // Mar 29 2023 20:00:00
    var expected = 1680134399999L; // one nanosecond before the start of the next date

    var actual = TimeConverter.toTimestampAtEndOfDay(timestamp);

    assertEquals(expected, actual);
  }

  @Test
  void should_convert_timestamp_to_date() {
    var timestamp = 1680120000000L; // Mar 29 2023 20:00:00
    var expected = "2023-03-29";

    var actual = TimeConverter.toUtcDate(timestamp);

    assertEquals(expected, actual);
  }
}