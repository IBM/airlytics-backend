package com.ibm.weather.airlytics.jobs.eventspatch.patcher;

import com.ibm.weather.airlytics.common.dto.AirlyticsEvent;

public class DoNothingPatcher implements AirlyticsEventPatcher {

    @Override
    public void patch(AirlyticsEvent event) {
        // DO NOTHING
    }
}
