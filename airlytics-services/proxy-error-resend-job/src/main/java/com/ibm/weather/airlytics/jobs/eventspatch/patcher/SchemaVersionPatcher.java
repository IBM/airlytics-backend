package com.ibm.weather.airlytics.jobs.eventspatch.patcher;

import com.ibm.weather.airlytics.common.dto.AirlyticsEvent;

public class SchemaVersionPatcher implements AirlyticsEventPatcher {

    private String targetSchema;

    public SchemaVersionPatcher(String targetSchema) {
        this.targetSchema = targetSchema;
    }

    @Override
    public void patch(AirlyticsEvent event) {
        event.setSchemaVersion(targetSchema);
    }
}
