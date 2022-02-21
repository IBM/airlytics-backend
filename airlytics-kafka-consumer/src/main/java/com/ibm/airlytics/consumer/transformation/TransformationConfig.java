package com.ibm.airlytics.consumer.transformation;

import java.util.List;

public class TransformationConfig {

    private String transformationId;
    private List<String> destinationTopics;
    private Integer percentageOfEvents;

    public String getTransformationId() {
        return transformationId;
    }

    public void setTransformationId(String transformationId) {
        this.transformationId = transformationId;
    }

    public List<String> getDestinationTopics() {
        return destinationTopics;
    }

    public void setDestinationTopics(List<String> destinationTopics) {
        this.destinationTopics = destinationTopics;
    }

    public Integer getPercentageOfEvents() {
        return percentageOfEvents;
    }

    public void setPercentageOfEvents(Integer percentageOfEvents) {
        this.percentageOfEvents = percentageOfEvents;
    }
}
