package com.ibm.airlytics.consumer.dsr.retriever;

import com.ibm.airlytics.consumer.dsr.DSRResponse;

import java.util.List;

public class DSRData {


    List<DSRResponse> usersData;
    ParquetRetriever.ParquetFields eventsFields;
    String dayPath;

    public DSRData(List<DSRResponse> usersData, ParquetRetriever.ParquetFields eventsFields, String dayPath) {
        this.usersData = usersData;
        this.eventsFields = eventsFields;
        this.dayPath = dayPath;
    }

    public String getDayPath() {
        return dayPath;
    }
    public List<DSRResponse> getUsersData() {
        return usersData;
    }

    public ParquetRetriever.ParquetFields getEventsFields() {
        return eventsFields;
    }



}
