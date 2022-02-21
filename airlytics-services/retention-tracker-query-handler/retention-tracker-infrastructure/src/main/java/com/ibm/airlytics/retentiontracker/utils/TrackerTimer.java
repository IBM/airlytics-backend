package com.ibm.airlytics.retentiontracker.utils;

public class TrackerTimer {
    private long startTime;
    private long currentStartTime;
    public TrackerTimer() {
        startTime = System.currentTimeMillis();
        currentStartTime = startTime;
    }

    public double getTime() {
        return getTimeMilli()/1000.0;
    }

    public long getTimeMilli() {
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - currentStartTime;
        currentStartTime = stopTime;
        return elapsedTime;
    }

    public long getTotalTimeMilli() {
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        return elapsedTime;
    }

    public double getTotalTime() {
        return getTotalTimeMilli()/1000.0;
    }

    public void reset() {
        startTime = System.currentTimeMillis();
        currentStartTime = startTime;
    }
}
