package com.ibm.weather.airlytics.jobs.dsr.dto;

import java.io.File;
import java.nio.file.Path;
import java.time.LocalDate;

public class DeletedUserMarker {

    private static final String SHARD_SUBPATH = "shard=";

    private static final String DAY_SUBPATH = "day=";

    private static final String USER_PREFIX = "_user_";

    private static final String DAY_PREFIX = "_day_";

    private Path markerPath;

    private String userId;

    private int shard;

    private LocalDate dsrDay;

    private LocalDate activityDay;

    public static DeletedUserMarker fromMarkerPath(Path markerPath) {
        DeletedUserMarker marker = new DeletedUserMarker();
        String sPath = markerPath.toString();

        int shard = extractShard(sPath);

        LocalDate activityDay = extractDay(sPath);

        //_user_%s_day_%s
        String fileName = sPath.substring(sPath.lastIndexOf(File.separator));
        String sRequestDay = fileName.substring(fileName.indexOf(DAY_PREFIX) + DAY_PREFIX.length());
        LocalDate dsrDay = LocalDate.parse(sRequestDay);

        String userId = fileName.substring(fileName.indexOf(USER_PREFIX) + USER_PREFIX.length(), fileName.indexOf(DAY_PREFIX));

        marker.setMarkerPath(markerPath);
        marker.setUserId(userId);
        marker.setShard(shard);
        marker.setDsrDay(dsrDay);
        marker.setActivityDay(activityDay);

        return marker;
    }

    public static LocalDate extractDay(String sPath) {
        int dayStart = sPath.indexOf(DAY_SUBPATH) + DAY_SUBPATH.length();
        String sDay = sPath.substring(dayStart, sPath.indexOf(File.separator, dayStart));
        LocalDate activityDay = LocalDate.parse(sDay);
        return activityDay;
    }

    public static int extractShard(String sPath) {
        int shardStart = sPath.indexOf(SHARD_SUBPATH) + SHARD_SUBPATH.length();
        int shardEnd = sPath.indexOf(File.separator, shardStart);

        if(shardEnd < 0) {
            shardEnd = sPath.length();
        }
        String sShard = sPath.substring(shardStart, shardEnd);
        int shard = Integer.valueOf(sShard);
        return shard;
    }

    public Path getMarkerPath() {
        return markerPath;
    }

    public void setMarkerPath(Path markerPath) {
        this.markerPath = markerPath;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public int getShard() {
        return shard;
    }

    public void setShard(int shard) {
        this.shard = shard;
    }

    public LocalDate getDsrDay() {
        return dsrDay;
    }

    public void setDsrDay(LocalDate dsrDay) {
        this.dsrDay = dsrDay;
    }

    public LocalDate getActivityDay() {
        return activityDay;
    }

    public void setActivityDay(LocalDate activityDay) {
        this.activityDay = activityDay;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DeletedUserMarker that = (DeletedUserMarker) o;

        if (shard != that.shard) return false;
        if (markerPath != null ? !markerPath.equals(that.markerPath) : that.markerPath != null) return false;
        if (userId != null ? !userId.equals(that.userId) : that.userId != null) return false;
        if (dsrDay != null ? !dsrDay.equals(that.dsrDay) : that.dsrDay != null) return false;
        return activityDay != null ? activityDay.equals(that.activityDay) : that.activityDay == null;
    }

    @Override
    public int hashCode() {
        int result = markerPath != null ? markerPath.hashCode() : 0;
        result = 31 * result + (userId != null ? userId.hashCode() : 0);
        result = 31 * result + shard;
        result = 31 * result + (dsrDay != null ? dsrDay.hashCode() : 0);
        result = 31 * result + (activityDay != null ? activityDay.hashCode() : 0);
        return result;
    }
}
