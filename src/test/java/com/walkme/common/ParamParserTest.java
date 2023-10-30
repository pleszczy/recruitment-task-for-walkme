package com.walkme.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.apache.flink.core.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ParamParserTest {

  private ParamParser sut;

  @BeforeEach
  public void setUp() {
    var args = new String[] {
        "--excludedActivityTypes", "walking,running",
        "--inputPath", "test_data/",
        "--outputPath", "test_output/"
    };
    sut = new ParamParser(args);
  }

  @Test
  public void should_return_excluded_activities_types() {
    var expected = Set.of("walking", "running");

    var actual = sut.excludeActivitiesTypes();

    assertEquals(expected, actual, "The excluded activities types should match the expected set.");
  }

  @Test
  public void should_return_excluded_activities_types_when_args_are_missing() {
    sut = new ParamParser(new String[] {});

    var actual = sut.excludeActivitiesTypes();

    assertTrue(actual.isEmpty(), "The excluded activities types should be empty for empty args.");
  }

  @Test
  public void should_return_input_path() {
    var expected = new Path("test_data/");

    var actual = sut.inputPath();

    assertEquals(expected, actual, "The input paths should match.");
  }

  @Test
  public void should_return_input_path_with_default_value() {
    sut = new ParamParser(new String[] {});
    var expected = new Path("data/");

    var actual = sut.inputPath();

    assertEquals(expected, actual, "The input path should default to 'data/'.");
  }

  @Test
  public void should_return_output_path() {
    var expected = new Path("test_output/");

    var actual = sut.outputPath();

    assertEquals(expected, actual, "The output paths should match.");
  }

  @Test
  public void should_return_output_path_with_default_value() {
    sut = new ParamParser(new String[] {});
    var expected = new Path("output/daily-activity");

    var actual = sut.outputPath();

    assertEquals(expected, actual, "The output path should default to 'output/daily-activity'.");
  }
}