package com.ibm.weather.airlytics.jobs.eventspatch.patcher;

import com.ibm.weather.airlytics.common.dto.AirlyticsEvent;

public interface AirlyticsEventPatcher {

    public void patch(AirlyticsEvent event);
}
