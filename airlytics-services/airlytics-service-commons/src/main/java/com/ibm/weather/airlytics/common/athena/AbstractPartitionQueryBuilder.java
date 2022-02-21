package com.ibm.weather.airlytics.common.athena;

public abstract class AbstractPartitionQueryBuilder extends AbstractBasicQueryBuilder implements QueryBuilder {

    protected int partition;

    public AbstractPartitionQueryBuilder(String athenaDb, String athenaTable, int partition) {
        super(athenaDb, athenaTable);
        this.partition = partition;
    }

    protected String getCondition() {
        return "partition = " + partition;
    }

    protected String getWherePart() {
        return " WHERE " + getCondition();
    }

    public int getPartition() {
        return partition;
    }
}
