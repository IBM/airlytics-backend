package com.ibm.weather.airlytics.jobs.eventspatch.db;

import com.ibm.weather.airlytics.common.athena.PartitionDayConditionQueryBuilder;
import com.ibm.weather.airlytics.common.athena.QueryBuilder;
import com.ibm.weather.airlytics.jobs.eventspatch.dto.EventsPatchAirlockConfig;

import java.time.LocalDate;

public class EventNameLikeQueryBuilder extends PartitionDayConditionQueryBuilder implements QueryBuilder {

    private String eventNameLike;

    public EventNameLikeQueryBuilder(
            String athenaDb,
            String athenaTable,
            int partition,
            LocalDate day,
            String eventNameLike) {
        super(
                athenaDb,
                athenaTable,
                partition,
                day,
                "name LIKE '" + eventNameLike + "'");
        this.eventNameLike = eventNameLike;
    }

    public EventNameLikeQueryBuilder(
            EventsPatchAirlockConfig featureConfig,
            int partition,
            LocalDate day,
            String eventNameLike) {
        this(featureConfig.getAthenaDb(), featureConfig.getAthenaTable(), partition, day, eventNameLike);
    }

    @Override
    protected String getSelectPart() {
        return "SELECT event";
    }

    public String getEventNameLike() {
        return eventNameLike;
    }
}
