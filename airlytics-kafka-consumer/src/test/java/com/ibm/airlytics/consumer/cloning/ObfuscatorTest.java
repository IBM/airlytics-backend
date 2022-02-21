package com.ibm.airlytics.consumer.cloning;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.ibm.airlytics.consumer.cloning.config.ObfuscationConfig;
import com.ibm.airlytics.consumer.cloning.obfuscation.Obfuscator;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ObfuscatorTest {

    private final static String PREFIX = "AAA";

    private final static String REGEX_UUID = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$";

    ObjectMapper mapper = new ObjectMapper();

    private final static String TEST_LOCATIONS_EVENT =
            "{\"eventId\":\"5E1BDA19-7F9C-43A3-ABD3-7E828626C491\"," +
                    "\"sessionId\":\"441FFD80-297F-4B3F-B981-13B5E3AB0B35\"," +
                    "\"appVersion\":\"11.10\"," +
                    "\"eventTime\":1586908921065," +
                    "\"schemaVersion\":\"1.0\"," +
                    "\"attributes\":{" +
                    "\"pushToken\":\"cfb84f12a926700ef4f4c4edb8b4a34a1e2bec79fafae489eff48fab0e2c3bb2\"," +
                    "\"locations\":" +
                    "[{\"lat\":31.7731372,\"lon\":35.2110909,\"name\":\"Jerusalem\"}," +
                    "{\"lat\":31.7653163,\"lon\":35.2180222,\"name\":\"Abu Tor\"}]" +
                    "}," +
                    "\"platform\":\"ios\"," +
                    "\"productId\":\"7900f30b-8f47-4829-95c5-9d4d6e8564ee\"," +
                    "\"userId\":\"EAA21680-14DE-4A5F-9F21-8B2F46702CA8\"," +
                    "\"name\":\"user-attributes\", \"customDimensions\": {\"dim\":\"custom\"}}";

    private static final String TEST_CONFIG =
                    "{\"hashPrefix\":\"AAA\"," +
                    "\"resendObfuscated\":false," +
                    "\"eventRules\":" +
                    "[{\"event\":\"*\",\"guidHash\":[\"userId\",\"sessionId\"]," +
                    "\"guidRandom\":[\"eventId\"]}," +
                    "{\"event\":\"user-attributes\"," +
                    "\"guidHash\":[\"attributes.pushToken\",\"attributes.upsId\"]," +
                    "\"geo\":[\"attributes.locations[*].lat\",\"attributes.locations[*].lon\"]}]}";

    @Test
    public void testFields() throws Exception {
        ObfuscationConfig oc = mapper.readValue(TEST_CONFIG, ObfuscationConfig.class);
        Obfuscator obf = new Obfuscator(oc);
        JsonNode event = mapper.readTree(TEST_LOCATIONS_EVENT);
        System.out.println(event.toString());
        obf.obfuscate(event);
        System.out.println(event.toString());

        JsonNode original = mapper.readTree(TEST_LOCATIONS_EVENT);

        // test GUID_RANDOM
        assertThat(event.get("eventId").asText()).matches(REGEX_UUID);
        assertThat(event.get("eventId").asText()).isNotEqualTo(original.get("eventId").asText());

        // test GUID_HASH
        assertThat(event.get("sessionId").asText()).matches(REGEX_UUID);
        assertThat(event.get("sessionId").asText()).startsWith(PREFIX);

        assertThat(event.get("userId").asText()).matches(REGEX_UUID);
        assertThat(event.get("userId").asText()).startsWith(PREFIX);

        JsonNode attrs = event.get("attributes");
        assertThat(attrs.get("pushToken").asText()).matches(REGEX_UUID);
        assertThat(attrs.get("pushToken").asText()).startsWith(PREFIX);

        ArrayNode locations = (ArrayNode) attrs.get("locations");
        ArrayNode originalLocations = (ArrayNode) original.get("attributes").get("locations");

        for(int i = 0; i < 2; i++) {
            JsonNode l = locations.get(i);
            JsonNode ol = originalLocations.get(i);

            assertThat(l.get("lat").doubleValue()).isNotEqualTo(ol.get("lat").doubleValue());
            assertThat(l.get("lon").doubleValue()).isNotEqualTo(ol.get("lon").doubleValue());

            assertThat(l.get("lat").doubleValue() - ol.get("lat").doubleValue()).isLessThan(1.0);
            assertThat(l.get("lon").doubleValue() - ol.get("lon").doubleValue()).isLessThan(1.0);
        }
    }
}
