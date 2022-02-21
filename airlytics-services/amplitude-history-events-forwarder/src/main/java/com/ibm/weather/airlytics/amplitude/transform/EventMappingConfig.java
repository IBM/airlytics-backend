package com.ibm.weather.airlytics.amplitude.transform;

import java.util.Map;
import java.util.StringJoiner;

public class EventMappingConfig {

    private String airlyticsEvent;

    private String thirdPartyEvent;

    private PropertyMappingConfig attributes;

    private PropertyMappingConfig customDimensions;

    public String getAirlyticsEvent() {
        return airlyticsEvent;
    }

    public void setAirlyticsEvent(String airlyticsEvent) {
        this.airlyticsEvent = airlyticsEvent;
    }

    public String getThirdPartyEvent() {
        return thirdPartyEvent;
    }

    public void setThirdPartyEvent(String thirdPartyEvent) {
        this.thirdPartyEvent = thirdPartyEvent;
    }

    public PropertyMappingConfig getAttributes() {
        return attributes;
    }

    public void setAttributes(PropertyMappingConfig attributes) {
        this.attributes = attributes;
    }

    public PropertyMappingConfig getCustomDimensions() {
        return customDimensions;
    }

    public void setCustomDimensions(PropertyMappingConfig customDimensions) {
        this.customDimensions = customDimensions;
    }

    @Deprecated
    public Map<String, String> getEventProperties() {
        return attributes != null ? attributes.getEventProperties() : null;
    }

    @Deprecated
    public void setEventProperties(Map<String, String> eventProperties) {

        if(eventProperties != null) {

            if(attributes == null) {
                attributes = new PropertyMappingConfig();
            }
            attributes.setEventProperties(eventProperties);
        } else {

            if(attributes != null) {
                attributes.setEventProperties(eventProperties);
            }
        }
    }

    @Deprecated
    public Map<String, String> getUserProperties() {
        return attributes != null ? attributes.getUserProperties() : null;
    }

    @Deprecated
    public void setUserProperties(Map<String, String> userProperties) {

        if(userProperties != null) {

            if(attributes == null) {
                attributes = new PropertyMappingConfig();
            }
            attributes.setUserProperties(userProperties);
        } else {

            if(attributes != null) {
                attributes.setUserProperties(userProperties);
            }
        }
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", EventMappingConfig.class.getSimpleName() + "[", "]")
                .add("airlyticsEvent='" + airlyticsEvent + "'")
                .add("thirdPartyEvent='" + thirdPartyEvent + "'")
                .add("attributes=" + attributes)
                .add("customDimensions=" + customDimensions)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EventMappingConfig that = (EventMappingConfig) o;

        if (airlyticsEvent != null ? !airlyticsEvent.equals(that.airlyticsEvent) : that.airlyticsEvent != null)
            return false;
        if (thirdPartyEvent != null ? !thirdPartyEvent.equals(that.thirdPartyEvent) : that.thirdPartyEvent != null)
            return false;
        if (attributes != null ? !attributes.equals(that.attributes) : that.attributes != null) return false;
        return customDimensions != null ? customDimensions.equals(that.customDimensions) : that.customDimensions == null;
    }

    @Override
    public int hashCode() {
        int result = airlyticsEvent != null ? airlyticsEvent.hashCode() : 0;
        result = 31 * result + (thirdPartyEvent != null ? thirdPartyEvent.hashCode() : 0);
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        result = 31 * result + (customDimensions != null ? customDimensions.hashCode() : 0);
        return result;
    }
}
