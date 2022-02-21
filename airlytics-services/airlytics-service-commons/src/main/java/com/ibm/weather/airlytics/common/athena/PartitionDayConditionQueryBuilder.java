package com.ibm.weather.airlytics.common.athena;

import java.time.LocalDate;

public class PartitionDayConditionQueryBuilder extends AbstractPartitionDayQueryBuilder implements QueryBuilder {

    private String mainCondition;

    public PartitionDayConditionQueryBuilder(String athenaDb, String athenaTable, int partition, LocalDate day, String mainCondition) {
        super(athenaDb, athenaTable, partition, day);
        this.mainCondition = mainCondition;
    }

    @Override
    protected String getCondition() {
        return super.getCondition() + " AND " + mainCondition;
    }

    @Override
    public String build() {
        return getSelectPart() + getFromPart() + getWherePart();
    }

    public String getMainCondition() {
        return mainCondition;
    }
}
