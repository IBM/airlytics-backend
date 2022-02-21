package com.ibm.airlytics.consumer.amplitude.transformation;

import com.ibm.airlytics.consumer.amplitude.dto.AmplitudeEvent;
import com.ibm.airlytics.consumer.integrations.dto.AirlyticsEvent;
import com.ibm.airlytics.consumer.integrations.mappings.AbstractAirlyticsEventTransformer;
import com.ibm.airlytics.consumer.integrations.mappings.EventMappingConfig;
import com.ibm.airlytics.consumer.integrations.mappings.GenericEventMappingConsumerConfig;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;

import java.time.*;
import java.time.format.TextStyle;
import java.util.*;

public class AmplitudeEventTransformer extends AbstractAirlyticsEventTransformer<AmplitudeEvent> {

    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(AmplitudeEventTransformer.class.getName());

    public static final String AIR_TYPE_USER_ATTRS = "user-attributes";

    public static final String AIR_TYPE_UNINSTALL = "uninstall-detected";

    public static final String AIR_TYPE_PAGEVIEW = "page-viewed";

    public static final String AIR_CD_TZ = "deviceTimeZone";

    public static final String AMP_TYPE_IDENTIFY = "$identify";

    public static final String AMP_TYPE_TRIGGER = "amplitude-cohort-exported";

    public static final String AMP_ATTR_UNINSTALLED = "isUninstalled";

    public static final String AMP_ATTR_LASTSEEN = "lastSeen";

    public AmplitudeEventTransformer(GenericEventMappingConsumerConfig config) {
        super(config);
    }

    @Override
    public boolean isValid(GenericEventMappingConsumerConfig config) {

        if(!super.isValid(config)) return false;

        Optional<EventMappingConfig> invalid =
                config.getEventMappings()
                        .stream()
                        .filter(c -> AMP_TYPE_IDENTIFY.equals(c.getThirdPartyEvent()) &&
                                ( (c.getAttributes() != null && c.getAttributes().getEventProperties() != null) ||
                                        (c.getCustomDimensions() != null && c.getCustomDimensions().getEventProperties() != null) ) )
                        .findAny();

        return !invalid.isPresent();
    }

    @Override
    public Optional<AmplitudeEvent> transform(AirlyticsEvent event) {

        if(config.getIgnoreEventTypes() != null) {

            if(config.getIgnoreEventTypes().contains(event.getName())) {
                // explicitly ignored
                return Optional.empty();
            }
        }

        if(config.getIncludeEventTypes() != null) {

            if(!config.getIncludeEventTypes().contains(event.getName())) {
                // not included
                return Optional.empty();
            }
        }
        EventMappingConfig mappingConfig = null;

        if(config.getEventMappings() != null) {

            mappingConfig =
                    config.getEventMappings()
                            .stream()
                            .filter(c -> c.getAirlyticsEvent().equals(event.getName()))
                            .findAny()
                            .orElse(null);
        }

        AmplitudeEvent result = buildThirdPartyEntity(event, mappingConfig);

        return Optional.ofNullable(result);
    }

    @Override
    protected AmplitudeEvent buildThirdPartyEntity(AirlyticsEvent event, EventMappingConfig mappingConfig) {
        AmplitudeEvent result = buildBasicAmplitudeEvent(event);

        Map<String, Object> attributes = event.getAttributes();

        Map<String, Object> eventProperties = null;
        Map<String, Object> userProperties = null;

        if(attributes != null) {

            if(event.getName().toLowerCase().startsWith(AIR_TYPE_USER_ATTRS)) {
                setBuiltInPropertiesForApps(event, result, attributes);
            }

            Map<String, Object> properties = preProcessAttributes(attributes, mappingConfig);

            if(mappingConfig != null) {
                Map<String, String> eventPropertyMappings =
                        mappingConfig.getAttributes() != null ? mappingConfig.getAttributes().getEventProperties() : null;
                Map<String, String> userPropertyMappings =
                        mappingConfig.getAttributes() != null ? mappingConfig.getAttributes().getUserProperties() : null;
                result.setEvent_type(mappingConfig.getThirdPartyEvent());

                eventProperties = applyPropertyMappings(properties, eventProperties, eventPropertyMappings);
                userProperties = applyPropertyMappings(properties, userProperties, userPropertyMappings);

            } else {
                eventProperties = properties;
            }
        }
        Map<String, Object> customDims = event.getCustomDimensions();

        if(customDims != null) {
            Map<String, Object> properties = preProcessCustomDims(customDims, mappingConfig);

            if(mappingConfig != null) {
                Map<String, String> eventPropertyMappings =
                        mappingConfig.getCustomDimensions() != null ? mappingConfig.getCustomDimensions().getEventProperties() : null;
                Map<String, String> userPropertyMappings =
                        mappingConfig.getCustomDimensions() != null ? mappingConfig.getCustomDimensions().getUserProperties() : null;
                result.setEvent_type(mappingConfig.getThirdPartyEvent());

                eventProperties = applyPropertyMappings(properties, eventProperties, eventPropertyMappings);
                userProperties = applyPropertyMappings(properties, userProperties, userPropertyMappings);

            } else if(config.isCustomDimsAcceptedByDefault()) {

                if(eventProperties == null) {
                    eventProperties = properties;
                } else {
                    eventProperties.putAll(properties);
                }
            }
        }
        Instant eventTimeInstant = Instant.ofEpochMilli(event.getEventTime());
        OffsetDateTime eventDeviceTime = null;

        if(eventProperties != null && !AMP_TYPE_IDENTIFY.equals(result.getEvent_type())) {
            eventProperties.put("session_guid", event.getSessionId());
            eventProperties.put("schema_version", event.getSchemaVersion());
            eventProperties.put("airlytics_event_id", event.getEventId());

            if(customDims != null && customDims.containsKey(AIR_CD_TZ)) {
                Object deviceTimeZoneObj = customDims.get(AIR_CD_TZ);

                if(deviceTimeZoneObj != null) {
                    Integer deviceTimeZoneSec = null;

                    if(deviceTimeZoneObj instanceof Integer) {
                        deviceTimeZoneSec = (Integer)deviceTimeZoneObj;
                    } else if(deviceTimeZoneObj instanceof Number) {
                        deviceTimeZoneSec = ((Number)deviceTimeZoneObj).intValue();
                    } else if(deviceTimeZoneObj instanceof String) {
                        try {
                            deviceTimeZoneSec = Integer.parseInt((String)deviceTimeZoneObj);
                        } catch(Exception e) {}
                    }

                    if(deviceTimeZoneSec != null) {
                        ZoneOffset deviceZoneOffset = ZoneOffset.ofTotalSeconds(deviceTimeZoneSec);
                        eventDeviceTime = eventTimeInstant.atOffset(deviceZoneOffset);
                        eventProperties.put("hourOfDay", eventDeviceTime.getHour());
                        eventProperties.put("dayOfWeek", eventDeviceTime.getDayOfWeek().getDisplayName(TextStyle.SHORT, Locale.US));
                    }
                }
            }
        }

        if(AIR_TYPE_UNINSTALL.equals(event.getName())) {// special case, add user attribute

            if(userProperties == null) {
                userProperties = new HashMap<>();
            }
            userProperties.put(AMP_ATTR_UNINSTALLED, Boolean.TRUE);
        }

        if(AIR_TYPE_PAGEVIEW.equals(event.getName())) {// special case, add user attribute

            if(userProperties == null) {
                userProperties = new HashMap<>();
            }
            userProperties.put(AMP_ATTR_UNINSTALLED, Boolean.FALSE);
        }

        List<String> lastSeenEvents = ((AmplitudeTransformationConsumerConfig)config).getLastSeenEvents();

        if(lastSeenEvents != null && lastSeenEvents.contains(event.getName())) {// special case, add user attribute

            if(userProperties == null) {
                userProperties = new HashMap<>();
            }
            userProperties.put(AMP_ATTR_LASTSEEN, eventTimeInstant.toString());
        }
        result.setEvent_properties(eventProperties);
        result.setUser_properties(userProperties);

        return result;
    }

    public AmplitudeEvent buildBasicAmplitudeEvent(AirlyticsEvent event) {
        AmplitudeEvent result = new AmplitudeEvent();

        result.setEvent_type(event.getName());
        if(event.getEventTime() != null) result.setTime(event.getEventTime());
        result.setUser_id(event.getUserId());
        result.setDevice_id(event.getUserId());
        result.setInsert_id(event.getEventId());
        result.setSession_id( (event.getSessionStartTime() == null) ? -1L : event.getSessionStartTime());
        result.setApp_version(event.getAppVersion());
        return result;
    }

    private void setBuiltInPropertiesForApps(AirlyticsEvent event, AmplitudeEvent result, Map<String, Object> attributes) {

        if (attributes.containsKey("carrier") &&
                attributes.containsKey("osVersion") &&
                attributes.containsKey("rawDeviceModel") &&
                attributes.containsKey("deviceManufacturer")) {

            result.setCarrier((String) attributes.get("carrier"));
            result.setOs_version((String) attributes.get("osVersion"));
            result.setDevice_model((String) attributes.get("rawDeviceModel"));
            result.setDevice_manufacturer((String) attributes.get("deviceManufacturer"));

            if ("ios".equalsIgnoreCase(event.getPlatform())) result.setPlatform("iOS");
            else if ("android".equalsIgnoreCase(event.getPlatform())) result.setPlatform("Android");
            else result.setPlatform("Web");

            if (attributes.containsKey("osName")) result.setOs_name((String) attributes.get("osName"));
            else result.setOs_name(result.getPlatform());

        } else if (attributes.containsKey("carrier") ||
                attributes.containsKey("osVersion") ||
                attributes.containsKey("rawDeviceModel") ||
                attributes.containsKey("deviceManufacturer")) {
            LOGGER.warn("Incomplete device properties in Airlytics event " + event.getEventId());
        }
        // language
        if(attributes.containsKey("deviceLanguage")) result.setLanguage((String) attributes.get("deviceLanguage"));
        // country
        if(attributes.containsKey("physicalCountry")) {
            result.setCountry((String) attributes.get("physicalCountry"));
        }
        else if(attributes.containsKey("deviceCountry")) {
            result.setCountry((String) attributes.get("deviceCountry"));
        }
        // region
        if(attributes.containsKey("physicalState")) result.setRegion((String) attributes.get("physicalState"));
    }

    private Map<String, Object> applyPropertyMappings(
            Map<String, Object> sourceProperties,
            Map<String, Object> targetProperties,
            Map<String, String> targetPropertyMappings) {

        Map<String, Object> result = targetProperties;

        if (targetPropertyMappings != null) {

            if(result == null) {
                result = new HashMap<>();
            }

            if ("*".equals(targetPropertyMappings.get("*"))) { // take all event attributes or custom dims and add to Amplitude event or user properties
                result.putAll(sourceProperties);
            } else {

                for (String airlyticsName : sourceProperties.keySet()) {
                    String amplitudeName = targetPropertyMappings.get(airlyticsName);

                    if (amplitudeName != null) {
                        result.put(amplitudeName, sourceProperties.get(airlyticsName));
                    }
                }
            }

            if(result.isEmpty()) {
                result = targetProperties;
            }
        }
        return result;
    }
}
