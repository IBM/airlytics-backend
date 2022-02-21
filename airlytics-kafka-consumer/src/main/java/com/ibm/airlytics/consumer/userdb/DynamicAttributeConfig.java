package com.ibm.airlytics.consumer.userdb;

public class DynamicAttributeConfig {
    private String type;
    private String dbColumn;
    private String dbTable = null; // Used to override the table name when relevant.

    public String getDbTable() { return dbTable; }

    public void setDbTable(String dbTable) { this.dbTable = dbTable; }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDbColumn() {
        return dbColumn;
    }

    public void setDbColumn(String dbColumn) {
        this.dbColumn = dbColumn;
    }
}
