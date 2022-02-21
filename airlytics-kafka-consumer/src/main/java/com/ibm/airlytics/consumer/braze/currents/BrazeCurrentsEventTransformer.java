package com.ibm.airlytics.consumer.braze.currents;

import com.ibm.airlytics.consumer.integrations.dto.AirlyticsEvent;
import com.ibm.airlytics.consumer.integrations.dto.ProductDefinition;
import com.ibm.airlytics.consumer.integrations.mappings.AbstractThirdPartyEventTransformer;
import com.ibm.airlytics.consumer.integrations.mappings.EventMappingConfig;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.*;
import java.util.stream.Collectors;

public class BrazeCurrentsEventTransformer extends AbstractThirdPartyEventTransformer {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(BrazeCurrentsEventTransformer.class.getName());

    public static final String BRAZE_EVENT_ID = "id";
    public static final String BRAZE_EVENT_TIME = "time";
    public static final String BRAZE_EXT_USER_ID = "external_user_id";

    public BrazeCurrentsEventTransformer(BrazeCurrentsConsumerConfig config) {
        super(config);
    }

    public Optional<AirlyticsEvent> transform(GenericRecord event, ProductDefinition product) {

        if(event == null) {
            return Optional.empty();
        }
        return transform(event.getSchema().getFullName(), event, product);
    }

    @Override
    protected AirlyticsEvent buildAirlyticsEvent(Object event, EventMappingConfig mappingConfig, ProductDefinition product) {

        if( mappingConfig == null ||
                mappingConfig.getAttributes()  == null ||
                mappingConfig.getAttributes().getEventProperties() == null ||
                mappingConfig.getAttributes().getEventProperties().isEmpty() ) {
            // we require explicit mappings for Braze Currents events
            return null;
        }
        BrazeCurrentsConsumerConfig consumerConfig = (BrazeCurrentsConsumerConfig) config;
        GenericRecord record = (GenericRecord)event;
        Schema schema = record.getSchema();
        List<String> fields = schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
        Map<String, Object> properties = new HashMap<>();

        for(String field : fields) {
            Object val = record.get(field);

            if(val != null && !(val instanceof Number)) {
                val = val.toString();
            }
            properties.put(field, val);
        }

        if(!properties.containsKey(BRAZE_EXT_USER_ID) || properties.get(BRAZE_EXT_USER_ID) == null) {
            LOGGER.warn("Missing Airlytics ID: " + record);
            return null;
        }

        AirlyticsEvent result = new AirlyticsEvent();

        result.setSchemaVersion(product.getSchemaVersion());
        result.setProductId(product.getId());
        result.setPlatform(product.getPlatform());
        result.setName(mappingConfig.getAirlyticsEvent());
        result.setEventId(properties.get(BRAZE_EVENT_ID).toString());
        result.setEventTime(((Integer)properties.get(BRAZE_EVENT_TIME)).longValue() * 1000L);
        result.setUserId(properties.get(BRAZE_EXT_USER_ID).toString());

        Map<String, Object>  attributes = processAttributes(properties, mappingConfig);
        setMappedProperties(result, attributes, mappingConfig.getAttributes().getEventProperties());

        return result;
    }

    private Map<String, Object> processAttributes(Map<String, Object> properties, EventMappingConfig mappingConfig) {

        if(properties != null) {
            Map<String, Object> result = new HashMap<>(properties);

            applyPropertyFilters(
                    result,
                    config.getIgnoreEventAttributes(),
                    config.getIncludeEventAttributes(),
                    (mappingConfig == null) ? null : mappingConfig.getAttributes());
            return result;
        }
        return Collections.emptyMap();
    }

    private void setMappedProperties(AirlyticsEvent result, Map<String, Object> properties, Map<String, String> mappings) {

        if(mappings != null) {
            result.setAttributes(new HashMap<>());

            for(String thirdPartyName : properties.keySet()) {
                String airlyticsName = mappings.get(thirdPartyName);

                if(airlyticsName != null) {
                    result.getAttributes().put(airlyticsName, properties.get(thirdPartyName));
                }
            }
        }
    }
}
