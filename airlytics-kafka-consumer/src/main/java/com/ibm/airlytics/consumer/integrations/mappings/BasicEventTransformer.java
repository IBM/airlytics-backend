package com.ibm.airlytics.consumer.integrations.mappings;

import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Abstract class containing common methods for all event transformers
 */
public class BasicEventTransformer {

    final protected GenericEventMappingConsumerConfig config;

    public BasicEventTransformer(GenericEventMappingConsumerConfig config) {

        if(!isValid(config)) throw new IllegalArgumentException("Invalid third party event mappings config");

        this.config = config;
    }

    public boolean isValid(GenericEventMappingConsumerConfig config) {

        if(config == null) return false;

        if(config.getEventMappings() != null) {
            Optional<EventMappingConfig> invalid =
                    config.getEventMappings()
                            .stream()
                            .filter(c -> StringUtils.isBlank(c.getAirlyticsEvent()))
                            .findAny();

            if (invalid.isPresent()) return false;

            invalid =
                    config.getEventMappings()
                            .stream()
                            .filter(c -> StringUtils.isBlank(c.getThirdPartyEvent()))
                            .findAny();

            return !invalid.isPresent();
        }
        return true;
    }

    protected void applyPropertyFilters(Map<String, ?> properties, List<String> ignored, List<String> included, PropertyMappingConfig mappings) {
        if (ignored != null) {
            ignored.forEach(aName -> {
                boolean isNotMappedInEventProps =
                        mappings == null ||
                                mappings.getEventProperties() == null ||
                                !mappings.getEventProperties().containsKey(aName);
                boolean isNotMappedInUserProps =
                        mappings == null ||
                                mappings.getUserProperties() == null ||
                                !mappings.getUserProperties().containsKey(aName);

                if (isNotMappedInEventProps && isNotMappedInUserProps) {
                    properties.remove(aName);
                }
            });
        }

        if (included != null) {
            List<String> removed =
                    properties.keySet()
                            .stream()
                            .filter(k -> {
                                boolean isIncluded = included.contains(k);

                                if (isIncluded) {
                                    return false;
                                }
                                boolean isNotMappedInEventProps =
                                        mappings == null ||
                                                mappings.getEventProperties() == null ||
                                                !mappings.getEventProperties().containsKey(k);
                                boolean isNotMappedInUserProps =
                                        mappings == null ||
                                                mappings.getUserProperties() == null ||
                                                !mappings.getUserProperties().containsKey(k);

                                return isNotMappedInEventProps && isNotMappedInUserProps;
                            })
                            .collect(Collectors.toList());
            removed.forEach(properties::remove);
        }
    }
}
