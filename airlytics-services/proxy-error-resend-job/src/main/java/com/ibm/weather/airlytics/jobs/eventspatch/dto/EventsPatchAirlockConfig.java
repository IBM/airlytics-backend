package com.ibm.weather.airlytics.jobs.eventspatch.dto;

import com.ibm.weather.airlytics.common.dto.BasicAthenaConfig;
import com.ibm.weather.airlytics.jobs.eventspatch.eventproxy.EventApiClientConfig;
import com.ibm.weather.airlytics.jobs.eventspatch.eventproxy.EventProxyIntegrationConfig;

import java.util.List;

public class EventsPatchAirlockConfig extends BasicAthenaConfig {

    private String patchName;

    private String athenaDb;

    private String athenaTable;

    private int athenaParallelThreads = 3;

    private int patchStartPartition = 0;

    private EventProxyIntegrationConfig eventProxyIntegrationConfig = new EventProxyIntegrationConfig();

    private EventApiClientConfig eventApiClientConfig = new EventApiClientConfig();

    public String getPatchName() {
        return patchName;
    }

    public void setPatchName(String patchName) {
        this.patchName = patchName;
    }

    public String getAthenaDb() {
        return athenaDb;
    }

    public void setAthenaDb(String athenaDb) {
        this.athenaDb = athenaDb;
    }

    public String getAthenaTable() {
        return athenaTable;
    }

    public void setAthenaTable(String athenaTable) {
        this.athenaTable = athenaTable;
    }

    public EventProxyIntegrationConfig getEventProxyIntegrationConfig() {
        return eventProxyIntegrationConfig;
    }

    public void setEventProxyIntegrationConfig(EventProxyIntegrationConfig eventProxyIntegrationConfig) {
        this.eventProxyIntegrationConfig = eventProxyIntegrationConfig;
    }

    public EventApiClientConfig getEventApiClientConfig() {
        return eventApiClientConfig;
    }

    public void setEventApiClientConfig(EventApiClientConfig eventApiClientConfig) {
        this.eventApiClientConfig = eventApiClientConfig;
    }

    public int getAthenaParallelThreads() {
        return athenaParallelThreads;
    }

    public void setAthenaParallelThreads(int athenaParallelThreads) {
        this.athenaParallelThreads = athenaParallelThreads;
    }

    public int getPatchStartPartition() {
        return patchStartPartition;
    }

    public void setPatchStartPartition(int patchStartPartition) {
        this.patchStartPartition = patchStartPartition;
    }
}
