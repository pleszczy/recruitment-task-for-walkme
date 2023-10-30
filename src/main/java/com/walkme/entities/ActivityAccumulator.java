package com.walkme.entities;

public record ActivityAccumulator(String date, String userId, String environment, String activityType, long runTime) {
}