package com.ibm.weather.airlytics.jobs.dsr.dto;

public class PiTableConfig {

    private String athenaDb;

    private String piTable;

    private String userIdColumn;

    private String sourceS3Bucket;

    private String sourceS3Folder;

    public String getAthenaDb() {
        return athenaDb;
    }

    public void setAthenaDb(String athenaDb) {
        this.athenaDb = athenaDb;
    }

    public String getPiTable() {
        return piTable;
    }

    public void setPiTable(String piTable) {
        this.piTable = piTable;
    }

    public String getSourceS3Bucket() {
        return sourceS3Bucket;
    }

    public void setSourceS3Bucket(String sourceS3Bucket) {
        this.sourceS3Bucket = sourceS3Bucket;
    }

    public String getSourceS3Folder() {
        return sourceS3Folder;
    }

    public void setSourceS3Folder(String sourceS3Folder) {
        this.sourceS3Folder = sourceS3Folder;
    }

    public String getUserIdColumn() {
        return userIdColumn;
    }

    public void setUserIdColumn(String userIdColumn) {
        this.userIdColumn = userIdColumn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PiTableConfig that = (PiTableConfig) o;

        if (athenaDb != null ? !athenaDb.equals(that.athenaDb) : that.athenaDb != null) return false;
        if (piTable != null ? !piTable.equals(that.piTable) : that.piTable != null) return false;
        if (userIdColumn != null ? !userIdColumn.equals(that.userIdColumn) : that.userIdColumn != null) return false;
        if (sourceS3Bucket != null ? !sourceS3Bucket.equals(that.sourceS3Bucket) : that.sourceS3Bucket != null)
            return false;
        return sourceS3Folder != null ? sourceS3Folder.equals(that.sourceS3Folder) : that.sourceS3Folder == null;
    }

    @Override
    public int hashCode() {
        int result = athenaDb != null ? athenaDb.hashCode() : 0;
        result = 31 * result + (piTable != null ? piTable.hashCode() : 0);
        result = 31 * result + (userIdColumn != null ? userIdColumn.hashCode() : 0);
        result = 31 * result + (sourceS3Bucket != null ? sourceS3Bucket.hashCode() : 0);
        result = 31 * result + (sourceS3Folder != null ? sourceS3Folder.hashCode() : 0);
        return result;
    }
}
