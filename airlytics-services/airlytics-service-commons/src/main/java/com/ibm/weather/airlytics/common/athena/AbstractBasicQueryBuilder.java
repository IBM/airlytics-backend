package com.ibm.weather.airlytics.common.athena;

public abstract class AbstractBasicQueryBuilder implements QueryBuilder {

    protected String athenaDb;

    protected String athenaTable;

    public AbstractBasicQueryBuilder(String athenaDb, String athenaTable) {
        this.athenaDb = athenaDb;
        this.athenaTable = athenaTable;
    }

    protected String getSourceSchemaTable() {
        return athenaDb + "." + athenaTable;
    }

    protected String getSelectPart() {
        return "SELECT *";
    }

    protected String getFromPart() {
        return " FROM " + getSourceSchemaTable();
    }

    public String getAthenaDb() {
        return athenaDb;
    }

    public String getAthenaTable() {
        return athenaTable;
    }
}
