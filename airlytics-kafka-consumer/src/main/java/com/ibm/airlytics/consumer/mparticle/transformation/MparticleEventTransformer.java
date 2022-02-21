package com.ibm.airlytics.consumer.mparticle.transformation;

import com.google.gson.Gson;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.integrations.dto.AirlyticsEvent;
import com.ibm.airlytics.consumer.integrations.mappings.AbstractAirlyticsEventTransformer;
import com.ibm.airlytics.consumer.integrations.mappings.EventMappingConfig;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import com.mparticle.model.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MparticleEventTransformer extends AbstractAirlyticsEventTransformer<Batch> {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(MparticleEventTransformer.class.getName());

    private static final String COMMERCE_EVENT = CommerceEvent.EventTypeEnum.COMMERCE_EVENT.getValue();
    private static final String CUSTOM_EVENT = CustomEvent.EventTypeEnum.CUSTOM_EVENT.getValue();

    private static final String USER_ATTRIBUTES_EVENT = "user-attributes-updated";

    private Batch.Environment environment;

    public MparticleEventTransformer(MparticleTransformationConsumerConfig config) {
        super(config);

        try {
            String deployment = AirlockManager.getDeployment();
            this.environment =
                    "EXTERNAL".equalsIgnoreCase(deployment) ?
                            Batch.Environment.PRODUCTION :
                            Batch.Environment.DEVELOPMENT;
            LOGGER.info("mParticle Environment: " + this.environment.getValue());
        } catch (IllegalArgumentException e) {
            this.environment = Batch.Environment.DEVELOPMENT;
        }
    }

    @Override
    protected Batch buildThirdPartyEntity(AirlyticsEvent event, EventMappingConfig mappingConfig) {
        Batch batch = new Batch();
        batch.environment(this.environment);
        batch.userIdentities(new UserIdentities().customerId(event.getUserId()));

        boolean isUserAttributes = event.getName().toLowerCase().startsWith(AIR_TYPE_USER_ATTRS);

        Map<String, Object> attributes = event.getAttributes();
        Object mpEvent = null;
        Map<String, String> eventProperties = null;

        if (mappingConfig != null) {

            if (COMMERCE_EVENT.equalsIgnoreCase(mappingConfig.getThirdPartyEvent())) {
                mpEvent = commerceEvent(event, eventProperties);
            } else if (CUSTOM_EVENT.equalsIgnoreCase(mappingConfig.getThirdPartyEvent())) {
                mpEvent = customEvent(event, eventProperties);
            }
        } else {
            mpEvent = customEvent(event, eventProperties);
        }

        if(attributes != null) {

            eventProperties = preProcessEventAttributes(attributes, mappingConfig);
            Map<String, Object> customDims = event.getCustomDimensions();

            if(customDims != null && config.isCustomDimsAcceptedByDefault()) {
                Map<String, String> cdProperties = preProcessEventCustomDims(customDims, mappingConfig);
                eventProperties.putAll(cdProperties);
            }

            if (isUserAttributes) {
                Map<String, Object> userProperties = preProcessUserAttributes(attributes, mappingConfig);
                setDeviceInfo(event, batch);
                batch.userAttributes(userProperties);

                if (mpEvent != null && mpEvent instanceof CustomEvent) {
                    ((CustomEvent)mpEvent).getData().eventName(USER_ATTRIBUTES_EVENT);
                }
            }
        } else if(isUserAttributes) {
            mpEvent = null;
            batch = null;
        }

        if(mpEvent != null) {
            populateCommonEventData(event, mpEvent, eventProperties);
            batch.addEventsItem(mpEvent);
        }

        if(batch != null && batch.getEvents() != null && !batch.getEvents().isEmpty()) {
            return batch;
        }
        return null;
    }

    private Map<String, Object> preProcessUserAttributes(Map<String, Object> properties, EventMappingConfig mappingConfig) {
        Map<String, Object> result = new HashMap<>();

        if(properties != null) {
            properties.forEach((key, value) -> preProcessUserProperty(result, key, value));
            applyPropertyFilters(
                    result,
                    config.getIgnoreEventAttributes(),
                    config.getIncludeEventAttributes(),
                    (mappingConfig == null) ? null : mappingConfig.getAttributes());
        }
        return result;
    }

    private void preProcessUserProperty(Map<String, Object> result, String key, Object value) {

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

    private Map<String, String> preProcessEventAttributes(Map<String, Object> properties, EventMappingConfig mappingConfig) {
        Map<String, String> result = new HashMap<>();

        if(properties != null) {
            properties.forEach((key, value) -> preProcessEventProperty(result, key, value));
            applyPropertyFilters(
                    result,
                    config.getIgnoreEventAttributes(),
                    config.getIncludeEventAttributes(),
                    (mappingConfig == null) ? null : mappingConfig.getAttributes());
        }
        return result;
    }

    protected Map<String, String> preProcessEventCustomDims(Map<String, Object> properties, EventMappingConfig mappingConfig) {
        Map<String, String> result = new HashMap<>();

        if(properties != null) {
            properties.forEach((key, value) -> preProcessEventProperty(result, key, value));
            applyPropertyFilters(
                    result,
                    config.getIgnoreCustomDimensions(),
                    config.getIncludeCustomDimensions(),
                    (mappingConfig == null) ? null : mappingConfig.getCustomDimensions());
        }
        return result;
    }

    private void preProcessEventProperty(Map<String, String> result, String key, Object value) {

        if(value != null) {

            if (key.toLowerCase().endsWith("date") || key.toLowerCase().endsWith("time")) { // possible date-time value
                convertDateTimeToString(result, key, value);
            } else if (value instanceof Map) { // JSON object value
                // this attribute will be ignored
            } else if (value instanceof List) { // JSON List
                preProcessListPropertyToString(result, key, value);
            } else {
                result.put(key, value.toString());
            }
        } else {
            result.put(key, null);
        }
    }

    private void convertDateTimeToString(Map<String, String> result, String key, Object value) {

        if(value instanceof Number) {
            Instant date = Instant.ofEpochMilli(((Number) value).longValue());
            ZonedDateTime zdt = ZonedDateTime.ofInstant(date, ZoneId.of("GMT"));

            if(zdt.getYear() > 2000 && zdt.getYear() < 2100) {
                result.put(key, date.toString());
                return;
            }
        }
        result.put(key, value.toString());
    }

    private void preProcessListPropertyToString(Map<String, String> result, String key, Object value) {
        List list = (List)value;

        if(!list.isEmpty()) {
            Object listValue = list.get(0);

            // convert lists of strings and numbers to comma-separated strings
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

    private void setDeviceInfo(AirlyticsEvent event, Batch batch) {
        DeviceInformation device = new DeviceInformation();
        batch.deviceInfo(device);

        if ("ios".equalsIgnoreCase(event.getPlatform())) device.platform(DeviceInformation.PlatformEnum.IOS);
        else if ("android".equalsIgnoreCase(event.getPlatform())) device.platform(DeviceInformation.PlatformEnum.ANDROID);
        else device.platform(DeviceInformation.PlatformEnum.WEB);

        Map<String, Object> attributes = event.getAttributes();

        if(attributes.containsKey("deviceCountry")) device.deviceCountry((String) attributes.get("deviceCountry"));
        if(attributes.containsKey("deviceLanguage")) device.localeLanguage((String) attributes.get("deviceLanguage"));
        if(attributes.containsKey("deviceManufacturer")) device.deviceManufacturer((String) attributes.get("deviceManufacturer"));
        if(attributes.containsKey("rawDeviceModel")) device.deviceModel((String) attributes.get("rawDeviceModel"));
        if(attributes.containsKey("osVersion")) device.osVersion((String) attributes.get("osVersion"));
        if(attributes.containsKey("carrier")) device.networkCarrier((String) attributes.get("carrier"));
    }

    private CommerceEvent commerceEvent(AirlyticsEvent aEvent, Map<String, String> properties) {
        CommerceEvent mpEvent = new CommerceEvent().data(new CommerceEventData());

        Map<String, Object> attributes = aEvent.getAttributes();
        MparticleTransformationConsumerConfig mpConfig = (MparticleTransformationConsumerConfig) config;

        if((attributes.get("premiumProductId") != null || attributes.get("newProductId") != null)) {

            if(mpConfig.isIgnoreTrialPurchases() && attributes.get("trial").equals(Boolean.TRUE)) {
                return null;
            }
            if(attributes.containsKey("screenType")) mpEvent.getData().screenName((String) attributes.get("screenType"));
            mpEvent.getData().currencyCode("USD");
            mpEvent.getData().productAction(new ProductAction().action(ProductAction.Action.PURCHASE));

            if("subscription-cancelled".equalsIgnoreCase(aEvent.getName())) {
                mpEvent.getData().getProductAction().action(ProductAction.Action.REFUND);
                mpEvent.getData().putCustomAttributesItem("purchaseType", "subscription-cancelled");
            }
            Product product = new Product();
            mpEvent.getData().getProductAction().addProductsItem(product);
            product.id((String)attributes.get("premiumProductId"));

            if(product.getId() == null) {
                product.id((String)attributes.get("newProductId"));
            }

            Object revenueObj = attributes.get("revenueUsdMicros");

            if(revenueObj == null) {
                product.totalProductAmount(BigDecimal.valueOf(0.0));
            } else if(revenueObj instanceof Number) {
                BigDecimal price = BigDecimal.valueOf(((Number) revenueObj).intValue() / 1_000_000.0);
                //price = price.setScale(2, RoundingMode.HALF_EVEN);
                product.totalProductAmount(price);
            } else {
                LOGGER.error("Invalid revenueUsdMicros: " + (new Gson()).toJson(aEvent));
                return null;
            }
            mpEvent.getData().getProductAction().totalAmount(product.getTotalProductAmount());

            if(product.getTotalProductAmount().doubleValue() < 0.0) {

                if(mpConfig.isIgnoreNegativePurchases()) {
                    return null;
                } else {
                    mpEvent.getData().getProductAction().action(ProductAction.Action.REFUND);
                }
            }

            // event properties for renew/upgrade
            if("subscription-renewed".equalsIgnoreCase(aEvent.getName())) {
                mpEvent.getData().putCustomAttributesItem("purchaseType", "subscription-renewed");
            } else if("subscription-upgraded".equalsIgnoreCase(aEvent.getName())) {
                mpEvent.getData().putCustomAttributesItem("purchaseType", "subscription-upgraded");
            }
        } else {
            LOGGER.error("Invalid subscription event: " + (new Gson()).toJson(aEvent));
            return null;
        }
        return mpEvent;
    }

    private CustomEvent customEvent(AirlyticsEvent aEvent, Map<String, String> properties) {
        CustomEvent mpEvent = new CustomEvent().data(new CustomEventData());
        mpEvent.getData().eventName(aEvent.getName());
        CustomEventData.CustomEventType subType = null;

        switch(aEvent.getName().toLowerCase()) {
            case "purchase-screen-viewed":
            case "location-viewed":
            case "card-clicked":
            case "asset-viewed":
            case "card-viewed":
                subType = CustomEventData.CustomEventType.NAVIGATION;
                break;
            case "alert-subscription-changed":
            case "ad-options-interactions":
                subType = CustomEventData.CustomEventType.USER_PREFERENCE;
                break;
            default: subType = CustomEventData.CustomEventType.OTHER;
        }
        mpEvent.getData().customEventType(subType);
        return mpEvent;
    }

    private void populateCommonEventData(AirlyticsEvent aEvent, Object mpEvent, Map<String, String> properties) {
        CommonEventData data = null;

        if (mpEvent != null) {

            if(mpEvent instanceof CustomEvent) {
                data = ((CustomEvent)mpEvent).getData();
            } else if(mpEvent instanceof CommerceEvent) {
                data = ((CommerceEvent)mpEvent).getData();
            }
        }

        if(data != null) {
            data.sourceMessageId(aEvent.getEventId());
            data.sessionUuid(aEvent.getSessionId());
            data.timestampUnixtimeMs(aEvent.getEventTime());
            data.putCustomAttributesItem("schemaVersion", aEvent.getSchemaVersion());
            data.putCustomAttributesItem("appVersion", aEvent.getAppVersion());

            if (properties != null) {
                data.getCustomAttributes().putAll(properties);
            }
        }
    }
}
