package com.ibm.weather.airlytics.jobs.eventspatch.patcher;

import com.ibm.weather.airlytics.common.athena.QueryBuilder;
import com.ibm.weather.airlytics.jobs.eventspatch.dto.EventsPatchAirlockConfig;

import java.time.LocalDate;
import java.util.LinkedList;
import java.util.List;

public abstract class AbstractPatch {

    public static List<LocalDate> getDays(LocalDate startDay, LocalDate endDay) {
        List<LocalDate> days = new LinkedList<>();
        LocalDate day = startDay;

        while(day.isBefore(endDay) || day.equals(endDay)) {
            days.add(day);
            day = day.plusDays(1L);
        }
        return days;
    }

    protected EventsPatchAirlockConfig featureConfig;

    protected AirlyticsEventPatcher eventPatcher;

    protected boolean failOnValidationError = true;

    public AbstractPatch(EventsPatchAirlockConfig featureConfig) {
        this.featureConfig = featureConfig;
    }

    public abstract QueryBuilder getPartiotionDayQueryBuilder(int partition, LocalDate day);

    public AirlyticsEventPatcher getEventPatcher() {
        return eventPatcher;
    }

    public abstract List<LocalDate> getDays();

    public boolean isFailOnValidationError() {
        return failOnValidationError;
    }
}
