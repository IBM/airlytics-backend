package com.ibm.airlytics.consumer.integrations.mappings;

import com.ibm.airlytics.consumer.integrations.dto.AirlyticsEvent;
import com.ibm.airlytics.consumer.integrations.dto.ProductDefinition;

import java.util.Optional;

/**
 * Generic implementation for Transformers creating Airlytics events from 3rd party events
 */
public abstract class AbstractThirdPartyEventTransformer extends BasicEventTransformer {

    public AbstractThirdPartyEventTransformer(GenericEventMappingConsumerConfig config) {
        super(config);
    }

    public Optional<AirlyticsEvent> transform(String eventType, Object event, ProductDefinition product) {

        if(event == null) {
            return Optional.empty();
        }

        if(config.getIgnoreEventTypes() != null && config.getIgnoreEventTypes().contains(eventType)) {
            // explicitly ignored
            return Optional.empty();
        }

        if(config.getIncludeEventTypes() != null && !config.getIncludeEventTypes().contains(eventType)) {
            // not included
            return Optional.empty();
        }
        EventMappingConfig mappingConfig = null;

        if(config.getEventMappings() != null) {

            mappingConfig =
                    config.getEventMappings()
                            .stream()
                            .filter(c -> c.getThirdPartyEvent().equals(eventType))
                            .findAny()
                            .orElse(null);
        }

        AirlyticsEvent result = buildAirlyticsEvent(event, mappingConfig, product);

        return Optional.ofNullable(result);
    }

    protected abstract AirlyticsEvent buildAirlyticsEvent(Object event, EventMappingConfig mappingConfig, ProductDefinition product);

}
