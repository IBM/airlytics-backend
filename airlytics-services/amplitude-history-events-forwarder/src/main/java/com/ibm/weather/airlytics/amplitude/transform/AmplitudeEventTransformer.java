package com.ibm.weather.airlytics.amplitude.transform;

import com.ibm.weather.airlytics.amplitude.dto.AirlyticsEvent;
import com.ibm.weather.airlytics.amplitude.dto.AirlyticsEventAppVersionComparator;
import com.ibm.weather.airlytics.amplitude.dto.AmplitudeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class AmplitudeEventTransformer extends AbstractAirlyticsEventTransformer<AmplitudeEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AmplitudeEventTransformer.class);

    public static final String AIR_TYPE_USER_ATTRS = "user-attributes";

    public static final String AIR_TYPE_UNINSTALL = "uninstall-detected";

    public static final String AIR_TYPE_PAGEVIEW = "page-viewed";

    public static final String AMP_TYPE_IDENTIFY = "$identify";

    public static final String AMP_TYPE_TRIGGER = "amplitude-cohort-exported";

    public static final String AMP_ATTR_UNINSTALLED = "isUninstalled";

    private static final List<String> ANDROID_PATCHED_EVENTS =
            Collections.unmodifiableList(
                    Arrays.asList("app-launch,asset-viewed,location-viewed".split("\\,")));

    private static final String ANDROID_MAX_VERSION_FOR_PATCH = "10.33.0";

    public AmplitudeEventTransformer() {
    }

    @Override
    public boolean isValid(GenericEventMappingConfig config) {

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

        boolean isPatchEnforced = isAndroid() && ANDROID_PATCHED_EVENTS.contains(event.getName()) && isVersionUpTo(event, ANDROID_MAX_VERSION_FOR_PATCH);

        if(!isPatchEnforced && config.getIgnoreEventTypes() != null) {

            if(config.getIgnoreEventTypes().contains(event.getName())) {
                // explicitly ignored
                return Optional.empty();
            }
        }

        if(!isPatchEnforced && config.getIncludeEventTypes() != null) {

            if(!config.getIncludeEventTypes().contains(event.getName())) {
                // not included
                return Optional.empty();
            }
        }
        EventMappingConfig mappingConfig = null;
        boolean isPatchedAppLaunch = isPatchEnforced && "app-launch".equalsIgnoreCase(event.getName());

        if(!isPatchedAppLaunch && config.getEventMappings() != null) {

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

                if(isWeb()) {
                    setBuiltInPropertiesForWeb(event, result, attributes);
                } else {
                    setBuiltInPropertiesForApps(event, result, attributes);
                }
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

        if(eventProperties != null && !AMP_TYPE_IDENTIFY.equals(result.getEvent_type())) {
            eventProperties.put("session_guid", event.getSessionId());
            eventProperties.put("schema_version", event.getSchemaVersion());
            eventProperties.put("airlytics_event_id", event.getEventId());
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

        boolean isPatchEnforced = isAndroid() && ANDROID_PATCHED_EVENTS.contains(event.getName()) && isVersionUpTo(event, ANDROID_MAX_VERSION_FOR_PATCH);
        boolean isPatchedAppLaunch = isPatchEnforced && "app-launch".equalsIgnoreCase(event.getName());

        if(isPatchedAppLaunch && attributes.containsKey("source")) {

            if(userProperties == null) {
                userProperties = new HashMap<>();
            }
            userProperties.put("launchSource", attributes.get("source"));
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

        if(attributes.containsKey("deviceLanguage")) result.setLanguage((String) attributes.get("deviceLanguage"));
        if(attributes.containsKey("deviceCountry")) result.setCountry((String) attributes.get("deviceCountry"));
    }

    private void setBuiltInPropertiesForWeb(AirlyticsEvent event, AmplitudeEvent result, Map<String, Object> attributes) {

        if(attributes.containsKey("language")) result.setLanguage((String) attributes.get("language"));

        if (attributes.containsKey("deviceModel") &&
                attributes.containsKey("osVersion") &&
                attributes.containsKey("osName")) {
            result.setPlatform("Web");
            result.setDevice_model((String) attributes.get("deviceModel"));
            result.setOs_version((String) attributes.get("osVersion"));
            result.setOs_name((String) attributes.get("osName"));
        } else if (attributes.containsKey("deviceModel") ||
                attributes.containsKey("osVersion") ||
                attributes.containsKey("osName")) {
            LOGGER.warn("Incomplete device properties in Airlytics event " + event.getEventId());
        }

        if (attributes.containsKey("geoCountry") &&
                attributes.containsKey("geoRegion") &&
                attributes.containsKey("geoCity") &&
                attributes.containsKey("geoDma")) {
            result.setCountry((String) attributes.get("geoCountry"));
            result.setRegion((String) attributes.get("geoRegion"));
            result.setCity((String) attributes.get("geoCity"));
            result.setDma((String) attributes.get("geoDma"));
        } else if (attributes.containsKey("geoCountry") ||
                attributes.containsKey("geoRegion") ||
                attributes.containsKey("geoCity") ||
                attributes.containsKey("geoDma")) {
            LOGGER.warn("Incomplete geo properties in Airlytics event " + event.getEventId());
        }

        if (attributes.containsKey("geoLatitude") && (attributes.get("geoLatitude") instanceof Number) &&
                attributes.containsKey("geoLongitude") && (attributes.get("geoLongitude") instanceof Number)) {
            result.setLocation_lat(((Number)attributes.get("geoLatitude")).floatValue());
            result.setLocation_lng(((Number)attributes.get("geoLongitude")).floatValue());
        } else if ((attributes.containsKey("geoLatitude") && !(attributes.get("geoLatitude") instanceof Number)) ||
                (attributes.containsKey("geoLongitude") && !(attributes.get("geoLongitude") instanceof Number))) {
            LOGGER.warn("Invalid location in event " + event.getEventId());
        }
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

    protected boolean isAndroid() {
        return airlyticsProduct.toLowerCase().contains("android");
    }

    protected boolean isVersionUpTo(AirlyticsEvent event, String maxVersionInclusive) {
        return AirlyticsEventAppVersionComparator.compareVersions(event.getAppVersion(), maxVersionInclusive) <= 0;
    }
}
