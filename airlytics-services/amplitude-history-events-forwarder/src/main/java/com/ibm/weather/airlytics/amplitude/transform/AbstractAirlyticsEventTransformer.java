package com.ibm.weather.airlytics.amplitude.transform;

import com.ibm.weather.airlytics.amplitude.dto.AirlyticsEvent;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Generic implementation for Transformers creating 3rd party events from Airlytics events
 * @param <T> destination event type
 */
public abstract class AbstractAirlyticsEventTransformer<T> extends BasicEventTransformer {

    public AbstractAirlyticsEventTransformer() {
    }

    public Optional<T> transform(AirlyticsEvent event) {

        if(config.getIgnoreEventTypes() != null && config.getIgnoreEventTypes().contains(event.getName())) {
            // explicitly ignored
            return Optional.empty();
        }

        if(config.getIncludeEventTypes() != null && !config.getIncludeEventTypes().contains(event.getName())) {
            // not included
            return Optional.empty();
        }
        T result = doTransform(event);

        return Optional.ofNullable(result);
    }

    protected T doTransform(AirlyticsEvent event) {
        EventMappingConfig mappingConfig = null;

        if(config.getEventMappings() != null) {

            mappingConfig =
                    config.getEventMappings()
                            .stream()
                            .filter(c -> c.getAirlyticsEvent().equals(event.getName()))
                            .findAny()
                            .orElse(null);
        }

        T result = buildThirdPartyEntity(event, mappingConfig);
        return result;
    }

    protected abstract T buildThirdPartyEntity(AirlyticsEvent event, EventMappingConfig mappingConfig);

    /**
     * Convert dates to ISO 8061 Strings, JSON arrays, ignore JSON properties,
     *  remove ignored properties, then retain only included
     */
    protected Map<String, Object> preProcessAttributes(Map<String, Object> properties, EventMappingConfig mappingConfig) {
        Map<String, Object> result = new HashMap<>();

        if(properties != null) {
            properties.forEach((key, value) -> preProcessProperty(result, key, value));
            applyPropertyFilters(
                    result,
                    config.getIgnoreEventAttributes(),
                    config.getIncludeEventAttributes(),
                    (mappingConfig == null) ? null : mappingConfig.getAttributes());
        }
        return result;
    }

    protected Map<String, Object> preProcessCustomDims(Map<String, Object> properties, EventMappingConfig mappingConfig) {
        Map<String, Object> result = new HashMap<>();

        if(properties != null) {
            properties.forEach((key, value) -> preProcessProperty(result, key, value));
            applyPropertyFilters(
                    result,
                    config.getIgnoreCustomDimensions(),
                    config.getIncludeCustomDimensions(),
                    (mappingConfig == null) ? null : mappingConfig.getCustomDimensions());
        }
        return result;
    }

    protected boolean isWeb() {
        return "Weather Web".equalsIgnoreCase(airlyticsProduct);
    }

    private void preProcessProperty(Map<String, Object> result, String key, Object value) {

        if (key.toLowerCase().endsWith("date") || key.toLowerCase().endsWith("time")) { // possible date-time value
            convertDateTime(result, key, value);
        } else if (value instanceof Map) { // JSON object value

            if (config.isJsonObjectAttributeAccepted()) {
                result.put(key, value);
            } // otherwise this attribute will be ignored
        } else if (value instanceof List) { // JSON List
            preProcessListProperty(result, key, value);
        } else {
            result.put(key, value);
        }
    }

    private void convertDateTime(Map<String, Object> result, String key, Object value) {

        if(value instanceof Number) {
            Instant date = Instant.ofEpochMilli(((Number) value).longValue());
            ZonedDateTime zdt = ZonedDateTime.ofInstant(date, ZoneId.of("GMT"));

            if(zdt.getYear() > 2000 && zdt.getYear() < 2100) {
                result.put(key, date.toString());
                return;
            }
        }
        result.put(key, value);
    }

    private void preProcessListProperty(Map<String, Object> result, String key, Object value) {
        List list = (List)value;

        if(config.isJsonArrayAttributeAccepted()) {

            if(!list.isEmpty()) {
                Object listValue = list.get(0);

                // allow lists of strings, numbers and, if configuration allows, JSONs
                if (listValue instanceof String || listValue instanceof Number || config.isJsonObjectAttributeAccepted()) {
                    result.put(key, value);
                }
            } else {
                result.put(key, value);
            }
        } else {

            if(!list.isEmpty()) {
                Object listValue = list.get(0);

                // convert lists of strings and numbers
                if(listValue instanceof String || listValue instanceof Number) {
                    StringBuilder sb = new StringBuilder();
                    list.forEach(o -> sb.append(o).append(','));
                    String strValue = sb.toString();
                    strValue = strValue.substring(0, strValue.length() - 1);
                    result.put(key, strValue);
                } // otherwise this attribute will be ignored
            } else {
                result.put(key, "");
            }
        }
    }
}
