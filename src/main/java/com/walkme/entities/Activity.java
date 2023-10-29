package com.walkme.entities;

public record Activity(String environment, String userId, String activityType, long startTimestamp, long endTimestamp) {
}