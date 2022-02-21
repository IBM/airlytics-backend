package com.ibm.airlytics.consumer.cloning.config;

import java.util.List;
import java.util.StringJoiner;

public class EventFieldsObfuscationConfig {

    private String event;

    private List<String> guidHash;

    private List<String> guidRandom;

    private List<String> geo;

    public String getEvent() { return event; }

    public void setEvent(String event) { this.event = event; }

    public List<String> getGuidHash() { return guidHash; }

    public void setGuidHash(List<String> guidHash) { this.guidHash = guidHash; }

    public List<String> getGuidRandom() { return guidRandom; }

    public void setGuidRandom(List<String> guidRandom) { this.guidRandom = guidRandom; }

    public List<String> getGeo() { return geo; }

    public void setGeo(List<String> geo) { this.geo = geo; }

    @Override
    public String toString() {
        return new StringJoiner(", ", EventFieldsObfuscationConfig.class.getSimpleName() + "[", "]")
                .add("event='" + event + "'")
                .add("guidHash=" + guidHash)
                .add("guidRandom=" + guidRandom)
                .add("geo=" + geo)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EventFieldsObfuscationConfig that = (EventFieldsObfuscationConfig) o;

        if (event != null ? !event.equals(that.event) : that.event != null) return false;
        if (guidHash != null ? !guidHash.equals(that.guidHash) : that.guidHash != null) return false;
        if (guidRandom != null ? !guidRandom.equals(that.guidRandom) : that.guidRandom != null) return false;
        return geo != null ? geo.equals(that.geo) : that.geo == null;
    }

    @Override
    public int hashCode() {
        int result = event != null ? event.hashCode() : 0;
        result = 31 * result + (guidHash != null ? guidHash.hashCode() : 0);
        result = 31 * result + (guidRandom != null ? guidRandom.hashCode() : 0);
        result = 31 * result + (geo != null ? geo.hashCode() : 0);
        return result;
    }
}
