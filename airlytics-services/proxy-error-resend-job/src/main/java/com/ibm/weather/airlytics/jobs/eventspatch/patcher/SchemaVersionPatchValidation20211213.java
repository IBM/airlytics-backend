package com.ibm.weather.airlytics.jobs.eventspatch.patcher;

import com.ibm.weather.airlytics.common.athena.QueryBuilder;
import com.ibm.weather.airlytics.jobs.eventspatch.db.EventNameSchemaVersionQueryBuilder;
import com.ibm.weather.airlytics.jobs.eventspatch.dto.EventsPatchAirlockConfig;

import java.time.LocalDate;
import java.util.List;

public class SchemaVersionPatchValidation20211213 extends AbstractPatch {

    private LocalDate startDay = LocalDate.parse("2021-12-14");

    private LocalDate endDay = LocalDate.parse("2021-12-14");

    private String eventName = "page-viewed";

    private String sourceSchemaVersion = "1.1";

    private String targetSchemaVersion = "666.0";

    private String otherConditions = "errormessage LIKE '%keyword\":\"additionalProperties%'";

    public SchemaVersionPatchValidation20211213(EventsPatchAirlockConfig featureConfig) {
        super(featureConfig);
        this.eventPatcher = new SchemaVersionPatcher(targetSchemaVersion);
        this.failOnValidationError = false;
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
