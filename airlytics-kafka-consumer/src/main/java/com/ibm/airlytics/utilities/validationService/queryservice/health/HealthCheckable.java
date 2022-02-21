package com.ibm.airlytics.utilities.validationService.queryservice.health;

public interface HealthCheckable {
    boolean isHealthy();
    String getHealthMessage();
}
