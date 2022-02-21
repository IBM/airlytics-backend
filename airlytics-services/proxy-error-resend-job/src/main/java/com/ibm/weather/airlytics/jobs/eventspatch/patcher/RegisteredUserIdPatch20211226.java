package com.ibm.weather.airlytics.jobs.eventspatch.patcher;

import com.ibm.weather.airlytics.common.athena.QueryBuilder;
import com.ibm.weather.airlytics.jobs.eventspatch.db.EventNameLikeQueryBuilder;
import com.ibm.weather.airlytics.jobs.eventspatch.dto.EventsPatchAirlockConfig;

import java.time.LocalDate;
import java.util.List;

public class RegisteredUserIdPatch20211226 extends AbstractPatch {

    private LocalDate startDay = LocalDate.parse("2021-12-26");

    private LocalDate endDay = LocalDate.parse("2021-12-29");

    private String eventNameLike = "braze-%";

    public RegisteredUserIdPatch20211226(EventsPatchAirlockConfig featureConfig) {
        super(featureConfig);
        this.eventPatcher = new DoNothingPatcher();
    }

    @Override
    public QueryBuilder getPartiotionDayQueryBuilder(int partition, LocalDate day) {
        return new EventNameLikeQueryBuilder(featureConfig, partition, day, eventNameLike);
    }

    @Override
    public List<LocalDate> getDays() {
        return getDays(startDay, endDay);
    }

    public LocalDate getStartDay() {
        return startDay;
    }

    public LocalDate getEndDay() {
        return endDay;
    }

    public String getEventNameLike() {
        return eventNameLike;
    }
}
