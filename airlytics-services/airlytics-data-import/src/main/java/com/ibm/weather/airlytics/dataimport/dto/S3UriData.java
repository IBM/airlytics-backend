package com.ibm.weather.airlytics.dataimport.dto;

import java.util.StringJoiner;

public class S3UriData {

    private String s3Region;

    private String s3Bucket;

    private String s3FilePath;

    public S3UriData() {
    }

    public S3UriData(String s3Region, String s3Bucket, String s3FilePath) {
        this.s3Region = s3Region;
        this.s3Bucket = s3Bucket;
        this.s3FilePath = s3FilePath;
    }

    public String getS3Region() {
        return s3Region;
    }

    public void setS3Region(String s3Region) {
        this.s3Region = s3Region;
    }

    public String getS3Bucket() {
        return s3Bucket;
    }

    public void setS3Bucket(String s3Bucket) {
        this.s3Bucket = s3Bucket;
    }

    public String getS3FilePath() {
        return s3FilePath;
    }

    public void setS3FilePath(String s3FilePath) {
        this.s3FilePath = s3FilePath;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", S3UriData.class.getSimpleName() + "[", "]")
                .add("s3Region='" + s3Region + "'")
                .add("s3Bucket='" + s3Bucket + "'")
                .add("s3FilePath='" + s3FilePath + "'")
                .toString();
    }
}
