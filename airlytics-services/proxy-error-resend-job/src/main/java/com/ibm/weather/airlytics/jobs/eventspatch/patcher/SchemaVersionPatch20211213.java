package com.ibm.weather.airlytics.jobs.eventspatch.patcher;

import com.ibm.weather.airlytics.common.athena.QueryBuilder;
import com.ibm.weather.airlytics.jobs.eventspatch.db.EventNameSchemaVersionQueryBuilder;
import com.ibm.weather.airlytics.jobs.eventspatch.dto.EventsPatchAirlockConfig;

import java.time.LocalDate;
import java.util.List;

public class SchemaVersionPatch20211213 extends AbstractPatch {

    private LocalDate startDay = LocalDate.parse("2021-12-13");

    private LocalDate endDay = LocalDate.parse("2021-12-19");

    private String eventName = "page-viewed";

    private String sourceSchemaVersion = "1.1";

    private String targetSchemaVersion = "1.4";

    private String otherConditions = "errormessage LIKE '%keyword\":\"additionalProperties%'";

    public SchemaVersionPatch20211213(EventsPatchAirlockConfig featureConfig) {
        super(featureConfig);
        this.eventPatcher = new SchemaVersionPatcher(targetSchemaVersion);
    }

    @Override
    public QueryBuilder getPartiotionDayQueryBuilder(int partition, LocalDate day) {
        return new EventNameSchemaVersionQueryBuilder(featureConfig, partition, day, eventName, sourceSchemaVersion, otherConditions);
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

    public String getEventName() {
        return eventName;
    }

    public String getSourceSchemaVersion() {
        return sourceSchemaVersion;
    }

    public String getTargetSchemaVersion() {
        return targetSchemaVersion;
    }

    public String getOtherConditions() {
        return otherConditions;
    }
}
