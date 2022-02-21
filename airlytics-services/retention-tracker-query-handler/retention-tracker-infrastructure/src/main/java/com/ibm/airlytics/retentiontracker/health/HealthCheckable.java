package com.ibm.airlytics.retentiontracker.health;

public interface HealthCheckable {
    public boolean isHealthy();
    public String getHealthMessage();
}
