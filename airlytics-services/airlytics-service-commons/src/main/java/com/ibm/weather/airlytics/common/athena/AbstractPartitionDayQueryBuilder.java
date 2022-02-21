package com.ibm.weather.airlytics.common.athena;

import java.time.LocalDate;

public abstract class AbstractPartitionDayQueryBuilder extends AbstractPartitionQueryBuilder implements QueryBuilder {

    protected LocalDate day;

    public AbstractPartitionDayQueryBuilder(String athenaDb, String athenaTable, int partition, LocalDate day) {
        super(athenaDb, athenaTable, partition);
        this.day = day;
    }

    @Override
    protected String getCondition() {
        return super.getCondition() + " AND day = '" + day + "'";
    }

    public LocalDate getDay() {
        return day;
    }
}
