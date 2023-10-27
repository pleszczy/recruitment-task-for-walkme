package com.walkme.entities;

/**
 * Named accumulator class for better code readability.
 */
public record ActivityAccumulator(String date, String userId, String environment, String activityType, long runTime) {
}