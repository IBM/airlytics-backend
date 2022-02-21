package com.ibm.analytics.queryservice.health;

public interface HealthCheckable {
    boolean isHealthy();
    String getHealthMessage();
}
