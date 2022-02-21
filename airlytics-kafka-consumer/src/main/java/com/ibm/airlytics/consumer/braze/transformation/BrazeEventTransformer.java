package com.ibm.airlytics.consumer.braze.transformation;

import com.google.gson.Gson;
import com.ibm.airlytics.consumer.braze.dto.BrazeEntity;
import com.ibm.airlytics.consumer.integrations.dto.AirlyticsEvent;
import com.ibm.airlytics.consumer.integrations.mappings.AbstractAirlyticsEventTransformer;
import com.ibm.airlytics.consumer.integrations.mappings.EventMappingConfig;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class BrazeEventTransformer extends AbstractAirlyticsEventTransformer<BrazeEntity> {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(BrazeEventTransformer.class.getName());

    public BrazeEventTransformer(BrazeTransformationConsumerConfig config) {
        super(config);
    }

    @Override
    protected BrazeEntity buildThirdPartyEntity(AirlyticsEvent event, EventMappingConfig mappingConfig) {
        BrazeEntity result = new BrazeEntity();
        BrazeTransformationConsumerConfig brazeConfig = (BrazeTransformationConsumerConfig)config;
        if(AirlyticsEvent.USER_ATTRIBUTES_EVENT.equals(event.getName())) {
            result.setKind(BrazeEntity.Kind.USER);
        }
        else if(brazeConfig.getPurchaseEvents() != null && brazeConfig.getPurchaseEvents().contains(event.getName().toLowerCase())) {
            result.setKind(BrazeEntity.Kind.PURCHASE);
        }
        else {
            result.setKind(BrazeEntity.Kind.EVENT);
        }

        if(mappingConfig != null && "USER".equalsIgnoreCase(mappingConfig.getThirdPartyEvent())) {
            result.setKind(BrazeEntity.Kind.USER);
        }

        if(mappingConfig != null && "PURCHASE".equalsIgnoreCase(mappingConfig.getThirdPartyEvent())) {
            result.setKind(BrazeEntity.Kind.PURCHASE);
        }
        result.setExternal_id(event.getUserId());

        Map<String, Object> attributes = event.getAttributes();
        Map<String, Object> properties = preProcessAttributes(attributes, mappingConfig);

        Map<String, String> eventPropertyMappings = null;
        Map<String, String> userPropertyMappings = null;

        if(mappingConfig != null) {
            eventPropertyMappings =
                    mappingConfig.getAttributes() != null ? mappingConfig.getAttributes().getEventProperties() : null;
            userPropertyMappings =
                    mappingConfig.getAttributes() != null ? mappingConfig.getAttributes().getUserProperties() : null;
        }

        if(result.getKind() == BrazeEntity.Kind.EVENT) {
            Instant date = Instant.ofEpochMilli(event.getEventTime());
            result.setTime(date.toString());// ISO 8061
            result.setName(mappingConfig != null ? mappingConfig.getThirdPartyEvent() : event.getName());
            String appId = brazeConfig.getBrazeAppId();

            if( StringUtils.isNotBlank(appId) ) {
                result.setApp_id(appId);
            }

            if("video-played".equalsIgnoreCase(event.getName())) {
                String playMethod = (String)event.getAttributes().get("playMethod");

                if("auto".equalsIgnoreCase(playMethod)) {
                    return null;
                }
            }
            setMappedProperties(result, properties, eventPropertyMappings, mappingConfig == null);
        } else if(result.getKind() == BrazeEntity.Kind.PURCHASE) {
            Instant date = Instant.ofEpochMilli(event.getEventTime());
            result.setTime(date.toString());

            if(attributes != null &&
                    (attributes.get("premiumProductId") != null || attributes.get("newProductId") != null)) {

                if(brazeConfig.isIgnoreTrialPurchases() && attributes.get("trial").equals(Boolean.TRUE)) {
                    return null;
                }
                result.setProduct_id((String)attributes.get("premiumProductId"));

                if(result.getProduct_id() == null) {
                    result.setProduct_id((String)attributes.get("newProductId"));
                }
                result.setCurrency("USD");
                Object revenueObj = attributes.get("revenueUsdMicros");

                if(revenueObj == null) {
                    result.setPrice(BigDecimal.valueOf(0.0));
                } else if(revenueObj instanceof Number) {
                    BigDecimal price = BigDecimal.valueOf(((Number) revenueObj).intValue() / 1_000_000.0);
                    //price = price.setScale(2, RoundingMode.HALF_EVEN);
                    result.setPrice(price);
                } else {
                    LOGGER.error("Invalid revenueUsdMicros: " + (new Gson()).toJson(event));
                    return null;
                }

                if(brazeConfig.isIgnoreNegativePurchases() && result.getPrice().doubleValue() < 0.0) {
                    return null;
                }
            } else {
                LOGGER.error("Invalid subscription event: " + (new Gson()).toJson(event));
                return null;
            }
            setMappedProperties(result, properties, eventPropertyMappings, mappingConfig == null);
            result.getProperties().put("type", event.getName());
        } else { // USER

            if("subscription-renewal-status-changed".equalsIgnoreCase(event.getName())) {
                Object status = event.getAttributes().get("status");
                Object previousStatus = event.getAttributes().get("previousStatus");

                if(status == null || status.equals(previousStatus)) {
                    return null;
                }
                properties.put("eventTime", Instant.ofEpochMilli(event.getEventTime()).toString());
            }
            setMappedProperties(result, properties, userPropertyMappings, mappingConfig == null);
        }

        if(result.getKind() == BrazeEntity.Kind.USER && result.toMap().size() <= 1) {
            // user-attributes event that is not relevant for Braze
            return null;
        }
        return result;
    }

    private void setMappedProperties(BrazeEntity result, Map<String, Object> properties, Map<String, String> mappings, boolean takeAllByDefault) {
        if(mappings != null) {

            if("*".equals(mappings.get("*"))) { // take all event attributes and add to Amplitude event properties
                result.setProperties(properties);
            } else {
                result.setProperties(new HashMap<>());

                for(String airlyticsName : properties.keySet()) {
                    String thirdPartyName = mappings.get(airlyticsName);

                    if(thirdPartyName != null) {
                        result.getProperties().put(thirdPartyName, properties.get(airlyticsName));
                    }
                }
            }
        } else if(takeAllByDefault) {
            result.setProperties(properties);
        }
    }
}
