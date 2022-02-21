package com.ibm.weather.airlytics.jobs.eventspatch.db;

import com.ibm.weather.airlytics.common.athena.PartitionDayConditionQueryBuilder;
import com.ibm.weather.airlytics.common.athena.QueryBuilder;
import com.ibm.weather.airlytics.jobs.eventspatch.dto.EventsPatchAirlockConfig;

import java.time.LocalDate;

public class EventNameSchemaVersionQueryBuilder extends PartitionDayConditionQueryBuilder implements QueryBuilder {

    private String eventName;

    private String schemaVersion;

    private String otherConditions;

    public EventNameSchemaVersionQueryBuilder(
            String athenaDb,
            String athenaTable,
            int partition,
            LocalDate day,
            String eventName,
            String schemaVersion,
            String otherConditions) {
        super(
                athenaDb,
                athenaTable,
                partition,
                day,
                "name = '" + eventName + "'" +
                        " AND schemaversion = '" + schemaVersion + "'" +
                        " AND " + otherConditions);
        this.eventName = eventName;
        this.schemaVersion = schemaVersion;
        this.otherConditions = otherConditions;
    }

    public EventNameSchemaVersionQueryBuilder(
            EventsPatchAirlockConfig featureConfig,
            int partition,
            LocalDate day,
            String eventName,
            String schemaVersion,
            String otherConditions) {
        this(featureConfig.getAthenaDb(), featureConfig.getAthenaTable(), partition, day, eventName, schemaVersion, otherConditions);
    }

    @Override
    protected String getSelectPart() {
        return "SELECT event";
    }

    public String getEventName() {
        return eventName;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }
}
