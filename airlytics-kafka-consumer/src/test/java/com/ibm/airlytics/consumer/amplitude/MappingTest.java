package com.ibm.airlytics.consumer.amplitude;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.consumer.integrations.dto.AirlyticsEvent;
import com.ibm.airlytics.consumer.amplitude.dto.AmplitudeEvent;
import com.ibm.airlytics.consumer.amplitude.transformation.AmplitudeEventTransformer;
import com.ibm.airlytics.consumer.amplitude.transformation.AmplitudeTransformationConsumerConfig;
import com.ibm.airlytics.consumer.integrations.mappings.EventMappingConfig;
import com.ibm.airlytics.consumer.integrations.mappings.PropertyMappingConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class MappingTest {

    final private ObjectMapper mapper = new ObjectMapper();

    public final static String TEST_LOCATIONS_EVENT =
            "{\"eventId\":\"5E1BDA19-7F9C-43A3-ABD3-7E828626C491\"," +
                    "\"sessionId\":\"441FFD80-297F-4B3F-B981-13B5E3AB0B35\"," +
                    "\"appVersion\":\"11.10\"," +
                    "\"eventTime\":1586908921065," +
                    "\"schemaVersion\":\"1.0\"," +
                    "\"attributes\":{" +
                    "\"pushToken\":\"cfb84f12a926700ef4f4c4edb8b4a34a1e2bec79fafae489eff48fab0e2c3bb2\"," +
                    "\"deviceModel\":\"iPod\"," +
                    "\"tags\":" +
                    "[\"tag1\", \"tag2\"]," +
                    "\"numbers\":" +
                    "[15.123, 0.456]," +
                    "\"locations\":" +
                    "[{\"lat\":31.7731372,\"lon\":35.2110909,\"name\":\"Jerusalem\"}," +
                    "{\"lat\":31.7653163,\"lon\":35.2180222,\"name\":\"Abu Tor\"}]," +
                    "\"someTime\":" + System.currentTimeMillis() + "," +
                    "\"someDate\":" + System.currentTimeMillis() + "," +
                    "\"invalidTime\":12345," +
                    "\"nullValue\":null" +
                    "}," +
                    "\"customDimensions\": {" +
                    "\"viewedWeatherCondition\": 29," +
                    "\"physicalState\": null," +
                    "\"localSevere\": false," +
                    "\"viewedCountry\": \"US\"," +
                    "\"ignoredCountry\": \"UK\"," +
                    "\"viewedStates\": [\"TX\"]" +
                    "}," +
                    "\"platform\":\"ios\"," +
                    "\"productId\":\"7900f30b-8f47-4829-95c5-9d4d6e8564ee\"," +
                    "\"userId\":\"EAA21680-14DE-4A5F-9F21-8B2F46702CA8\"," +
                    "\"name\":\"user-attributes\"}";

    @Test
    public void testFields() throws Exception {

        AmplitudeTransformationConsumerConfig config = new AmplitudeTransformationConsumerConfig();

        config.setIgnoreEventAttributes(Arrays.asList("osVersion", "deviceModel", "deviceLanguage", "pushToken", "upsId", "thirdPartyId"));

        EventMappingConfig mappingConfig = new EventMappingConfig();
        mappingConfig.setAirlyticsEvent("user-attributes");
        mappingConfig.setThirdPartyEvent(AmplitudeEventTransformer.AMP_TYPE_IDENTIFY);
        mappingConfig.setAttributes(new PropertyMappingConfig());
        mappingConfig.getAttributes().setUserProperties(Collections.singletonMap("*", "*"));
        config.setEventMappings(Collections.singletonList(mappingConfig));

        AmplitudeEventTransformer transformer = new AmplitudeEventTransformer(config);
        AirlyticsEvent event = mapper.readValue(TEST_LOCATIONS_EVENT, AirlyticsEvent.class);
        assertNotNull(event);
        String eventJson = mapper.writeValueAsString(event);
        System.out.println(eventJson);
        assertFalse(eventJson.contains("registeredUserId"));
        AmplitudeEvent aEvent = transformer.transform(event).orElse(null);
        assertNotNull(aEvent);
        System.out.println(mapper.writeValueAsString(aEvent));
        assertNotNull(aEvent.getUser_properties());
        assertEquals(7, aEvent.getUser_properties().size());
        assertNull(aEvent.getEvent_properties());

        // test filtering out JSON attributes
        config.setJsonObjectAttributeAccepted(false);
        aEvent = transformer.transform(event).orElse(null);
        assertNotNull(aEvent);
        System.out.println(mapper.writeValueAsString(aEvent));
        assertNotNull(aEvent.getUser_properties());
        assertEquals(6, aEvent.getUser_properties().size());
        assertNull(aEvent.getEvent_properties());

        // test conversion of JSON arrays
        config.setJsonArrayAttributeAccepted(false);
        aEvent = transformer.transform(event).orElse(null);
        assertNotNull(aEvent);
        System.out.println(mapper.writeValueAsString(aEvent));
        assertNotNull(aEvent.getUser_properties());
        assertEquals(6, aEvent.getUser_properties().size());
        assertTrue(aEvent.getUser_properties().get("numbers") instanceof String);
        assertTrue(aEvent.getUser_properties().get("tags") instanceof String);
        assertNull(aEvent.getEvent_properties());

        // test custom dimensions
        config.setJsonArrayAttributeAccepted(true);
        // custom dims ignored by default
        config.setCustomDimsAcceptedByDefault(false);
        event.setName("test");
        aEvent = transformer.transform(event).orElse(null);
        assertNotNull(aEvent);
        System.out.println(mapper.writeValueAsString(aEvent));
        assertNull(aEvent.getUser_properties());
        assertNotNull(aEvent.getEvent_properties());
        assertFalse(aEvent.getEvent_properties().containsKey("viewedCountry"));
        assertEquals(9, aEvent.getEvent_properties().size()); // 2+6+airlytics_event_id

        // custom dims accepted by default
        config.setCustomDimsAcceptedByDefault(true);
        // always ignore "ignoredCountry"
        config.setIgnoreCustomDimensions(Arrays.asList("ignoredCountry"));
        aEvent = transformer.transform(event).orElse(null);
        assertNotNull(aEvent);
        System.out.println(mapper.writeValueAsString(aEvent));
        assertNull(aEvent.getUser_properties());
        assertNotNull(aEvent.getEvent_properties());
        assertFalse(aEvent.getEvent_properties().containsKey("ignoredCountry"));
        assertTrue(aEvent.getEvent_properties().containsKey("viewedStates"));
        assertTrue(aEvent.getEvent_properties().get("viewedStates") instanceof List);
        assertTrue(aEvent.getEvent_properties().containsKey("physicalState"));
        assertNull(aEvent.getEvent_properties().get("physicalState"));
        assertEquals(14, aEvent.getEvent_properties().size()); // 2+6+5+airlytics_event_id

        // custom dims mappings are missing from event mappings
        event.setName("user-attributes");
        aEvent = transformer.transform(event).orElse(null);
        assertNotNull(aEvent);
        System.out.println(mapper.writeValueAsString(aEvent));
        assertNotNull(aEvent.getUser_properties());
        assertNull(aEvent.getEvent_properties());
        assertFalse(aEvent.getUser_properties().containsKey("viewedCountry"));;
        assertEquals(6, aEvent.getUser_properties().size()); // 6

        // custom dims mappings, asterisk
        mappingConfig.setCustomDimensions(new PropertyMappingConfig());
        mappingConfig.getCustomDimensions().setUserProperties(Collections.singletonMap("*", "*"));
        aEvent = transformer.transform(event).orElse(null);
        assertNotNull(aEvent);
        System.out.println(mapper.writeValueAsString(aEvent));
        assertNotNull(aEvent.getUser_properties());
        assertNull(aEvent.getEvent_properties());
        assertTrue(aEvent.getUser_properties().containsKey("viewedCountry"));
        assertFalse(aEvent.getUser_properties().containsKey("ignoredCountry"));
        assertTrue(aEvent.getUser_properties().containsKey("physicalState"));
        assertNull(aEvent.getUser_properties().get("physicalState"));;
        assertEquals(11, aEvent.getUser_properties().size()); // 6+5

        // custom dims explicit mappings
        mappingConfig.getCustomDimensions().setUserProperties(Collections.singletonMap("viewedWeatherCondition", "transformedWeatherCondition"));
        aEvent = transformer.transform(event).orElse(null);
        assertNotNull(aEvent);
        System.out.println(mapper.writeValueAsString(aEvent));
        assertNotNull(aEvent.getUser_properties());
        assertNull(aEvent.getEvent_properties());
        assertFalse(aEvent.getUser_properties().containsKey("viewedCountry"));
        assertFalse(aEvent.getUser_properties().containsKey("ignoredCountry"));
        assertTrue(aEvent.getUser_properties().containsKey("transformedWeatherCondition"));;
        assertEquals(7, aEvent.getUser_properties().size()); // 6+1
    }

    private final static String TEST_UPDATED_EVENT =
            "{\"eventId\":\"721acaff-dbbf-4b0a-9bff-56eecbf41326\",\"productId\":\"bc479db5-ff58-4138-b5e4-a8400a1f78d5\"," +
                    "\"appVersion\":\"10.20.0\",\"schemaVersion\":\"14.0\",\"name\":\"user-attributes\"," +
                    "\"userId\":\"95bc5001-4834-419d-9eaf-4486917d942b\"," +
                    "\"sessionId\":\"9cb9a736-9938-42fa-8bb7-937c489959fb\",\"sessionStartTime\":1601063685914,\"eventTime\":1601063692541," +
                    "\"platform\":\"android\"," +
                    "\"attributes\":{\"upsId\":\"TL0Qoz5-aljvO\",\"locationAuthorization\":\"always\"," +
                    "\"premiumProductId\":\"com.renewing.1month.pro\"}," +
                    "\"encodedReceipt\":\"receipt\"}," +
                    "\"previousValues\":{\"locationAuthorization\":\"denied\"}}";

    @Test
    public void setTestUpdatedEvent() throws Exception {
        AirlyticsEvent event = mapper.readValue(TEST_UPDATED_EVENT, AirlyticsEvent.class);
        assertNotNull(event);

        AmplitudeTransformationConsumerConfig config = new AmplitudeTransformationConsumerConfig();

        config.setIgnoreEventAttributes(Arrays.asList("osVersion", "deviceModel", "deviceLanguage", "pushToken", "upsId", "thirdPartyId", "premiumProductId", "encodedReceipt"));

        EventMappingConfig mappingConfig = new EventMappingConfig();
        mappingConfig.setAirlyticsEvent("user-attributes");
        mappingConfig.setThirdPartyEvent(AmplitudeEventTransformer.AMP_TYPE_IDENTIFY);
        mappingConfig.setAttributes(new PropertyMappingConfig());
        mappingConfig.getAttributes().setUserProperties(Collections.singletonMap("*", "*"));
        config.setEventMappings(Collections.singletonList(mappingConfig));

        AmplitudeEventTransformer transformer = new AmplitudeEventTransformer(config);
        AmplitudeEvent aEvent = transformer.transform(event).orElse(null);
        assertNotNull(aEvent);
        System.out.println(mapper.writeValueAsString(aEvent));
        assertNotNull(aEvent.getUser_properties());
        assertEquals(1, aEvent.getUser_properties().size());
        assertTrue(aEvent.getUser_properties().containsKey("locationAuthorization"));
        assertTrue(!aEvent.getUser_properties().containsKey("premiumProductId"));
        assertNull(aEvent.getEvent_properties());

        mappingConfig.setAttributes(new PropertyMappingConfig());
        mappingConfig.getAttributes().setUserProperties(Collections.singletonMap("premiumProductId", "premiumProductId"));
        aEvent = transformer.transform(event).orElse(null);
        assertNotNull(aEvent);
        System.out.println(mapper.writeValueAsString(aEvent));
        assertNotNull(aEvent.getUser_properties());
        assertEquals(1, aEvent.getUser_properties().size());
        assertTrue(!aEvent.getUser_properties().containsKey("locationAuthorization"));
        assertTrue(aEvent.getUser_properties().containsKey("premiumProductId"));
        assertNull(aEvent.getEvent_properties());
    }
}
