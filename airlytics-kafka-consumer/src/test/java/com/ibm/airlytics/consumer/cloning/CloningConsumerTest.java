package com.ibm.airlytics.consumer.cloning;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.airlytics.consumer.cloning.config.CloningConsumerConfig;
import com.ibm.airlytics.consumer.cloning.config.ObfuscationConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class CloningConsumerTest {
    private static final String TEST_CONFIG =
        "{\"hashPrefix\":\"aaa\"," +
                "\"resendObfuscated\":false," +
                "\"eventRules\":" +
                "[{\"event\":\"*\",\"guidHash\":[\"userId\",\"sessionId\"]," +
                "\"guidRandom\":[\"eventId\"]}," +
                "{\"event\":\"user-attributes\"," +
                "\"guidHash\":[\"attributes.pushToken\",\"attributes.upsId\"]," +
                "\"geo\":[\"attributes.locations[*].lat\",\"attributes.locations[*].lon\"]}]}";

    private final static String PREFIX = "aaa";

    ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testProcessRecord() throws Exception {
        CloningConsumerConfig config = createBasicConsumerConfig();
        config.setPercentageUsersCloned(11);
        ObfuscationConfig oc = mapper.readValue(TEST_CONFIG, ObfuscationConfig.class);
        config.getEventProxyIntegrationConfig().setObfuscation(oc);

        CloningConsumer consumer = new CloningConsumer();
        consumer.setConfig(config);
        consumer.init();

        final List<ConsumerRecord<String, JsonNode>> noHeadersBatch = new LinkedList<>();
        final List<ConsumerRecord<String, JsonNode>> headersBatch = new LinkedList<>();

        IntStream.rangeClosed(0, 99)
                .forEach(i -> processRecord(consumer, newRecord(i), noHeadersBatch, headersBatch));

        processRecord(consumer, noHeadersBatch.get(0), noHeadersBatch, headersBatch); // reprocess already processed

        assertEquals(11, noHeadersBatch.size());
        noHeadersBatch.forEach(r -> validateObfuscated(r.value()));

        validateBatchConversion(consumer, noHeadersBatch);
    }

    @Test
    public void testProcessRecordFilteringInclude() throws Exception {
        CloningConsumerConfig config = createBasicConsumerConfig();
        config.setPercentageUsersCloned(100);
        config.setIncludeEventTypes(Arrays.asList("user-attributes"));
        config.setIncludeEventAttributes(Arrays.asList("test-included"));

        CloningConsumer consumer = new CloningConsumer();
        consumer.setConfig(config);
        consumer.init();

        final List<ConsumerRecord<String, JsonNode>> noHeadersBatch = new LinkedList<>();
        final List<ConsumerRecord<String, JsonNode>> headersBatch = new LinkedList<>();
        Map<String, String> eventAttibutes = new HashMap<>();
        eventAttibutes.put("test-included", "1");
        eventAttibutes.put("test-excluded", "2");

        processRecord(consumer, newRecord(0, "user-attributes", eventAttibutes, null), noHeadersBatch, headersBatch);
        processRecord(consumer, newRecord(1, "test-event", null, null), noHeadersBatch, headersBatch);

        assertEquals(1, noHeadersBatch.size());
        JsonNode event = noHeadersBatch.get(0).value();
        assertEquals("user-attributes", event.get("name").textValue());
        ObjectNode eventAttributes = (ObjectNode)event.findValue("attributes");
        assertNotNull(eventAttributes);

        Iterator<String> eventFieldsIter = eventAttributes.fieldNames();
        List<String> attributeNames = new LinkedList<>();

        while (eventFieldsIter.hasNext()) {
            attributeNames.add(eventFieldsIter.next());
        }
        assertEquals(1, attributeNames.size());
        assertTrue(attributeNames.contains("test-included"));
    }

    @Test
    public void testProcessRecordFilteringIgnore() throws Exception {
        CloningConsumerConfig config = createBasicConsumerConfig();
        config.setPercentageUsersCloned(100);
        config.setIgnoreEventTypes(Arrays.asList("test-event"));
        config.setIgnoreEventAttributes(Arrays.asList("test-excluded"));

        CloningConsumer consumer = new CloningConsumer();
        consumer.setConfig(config);
        consumer.init();

        final List<ConsumerRecord<String, JsonNode>> noHeadersBatch = new LinkedList<>();
        final List<ConsumerRecord<String, JsonNode>> headersBatch = new LinkedList<>();
        Map<String, String> eventAttibutes = new HashMap<>();
        eventAttibutes.put("test-included", "1");
        eventAttibutes.put("test-excluded", "2");

        processRecord(consumer, newRecord(0, "user-attributes", eventAttibutes, null), noHeadersBatch, headersBatch);
        processRecord(consumer, newRecord(1, "test-event", null, null), noHeadersBatch, headersBatch);

        assertEquals(1, noHeadersBatch.size());
        JsonNode event = noHeadersBatch.get(0).value();
        assertEquals("user-attributes", event.get("name").textValue());
        ObjectNode eventAttributes = (ObjectNode)event.findValue("attributes");
        assertNotNull(eventAttributes);

        Iterator<String> eventFieldsIter = eventAttributes.fieldNames();
        List<String> attributeNames = new LinkedList<>();

        while (eventFieldsIter.hasNext()) {
            attributeNames.add(eventFieldsIter.next());
        }
        assertEquals(1, attributeNames.size());
        assertTrue(attributeNames.contains("test-included"));
    }

    private CloningConsumerConfig createBasicConsumerConfig() {
        CloningConsumerConfig config = new CloningConsumerConfig();
        config.getEventApiClientConfig().setEventApiBaseUrl("http://localhost:8080");
        config.getEventApiClientConfig().setEventApiPath("/test");
        config.getEventApiClientConfig().setEventApiKey("TEST");
        return config;
    }

    private static final String TEST_202_RESPONSE_BODY =
            "[" +
                    "{\"eventId\":\"A5AEFEA1-4F09-4072-82E4-715A35D8733A\",\"errorType\":\"fetch event validator\",\"shouldRetry\":false," +
                    "\"error\":{\"message\":\"The specified key does not exist.\",\"code\":\"NoSuchKey\",\"region\":null,\"time\":\"2020-05-21T16:14:25.834Z\"," +
                    "\"requestId\":\"A78410547CC22028\",\"extendedRequestId\":\"kDsWMtxUmsVoWh0R66z+X37IIov2s5gH7hl/+WB3xF3LzMPGYWy6x8njmdOZquHmALIHwxQNmrs=\",\"statusCode\":404," +
                    "\"retryable\":false,\"retryDelay\":45.622445278000924,\"level\":\"error\",\"timestamp\":\"2020-05-21T16:14:25.834Z\"}}," +
                    "{\"eventId\":\"D90C68D3-4E1F-416C-AA5E-DF44BF552690\",\"errorType\":\"fetch event validator\",\"shouldRetry\":true," +
                    "\"error\":{\"message\":\"The specified key does not exist.\",\"code\":\"NoSuchKey\",\"region\":null,\"time\":\"2020-05-21T16:14:25.871Z\"," +
                    "\"requestId\":\"6D3AF176C9E90E6D\",\"extendedRequestId\":\"I7PkW7uoM3FYMaDecRbYfCZsWoK14YsT3Eh+BwjZkZ2CPI0CWaLjb0562wATWbd7FCVoBnmb26I=\",\"statusCode\":404," +
                    "\"retryable\":false,\"retryDelay\":56.48775904618415,\"level\":\"error\",\"timestamp\":\"2020-05-21T16:14:25.871Z\"}}," +
                    "{\"eventId\":\"D90C68D3-4E1F-416C-AA5E-DF44BF552691\"}" +
            "]";

    @Test
    public void test202ResponseParsing() throws Exception {
        String responseBody = TEST_202_RESPONSE_BODY;
        JsonNode responseJson = mapper.readTree(responseBody);
        assertTrue(responseJson instanceof ArrayNode);
        List<String> retries = new LinkedList<>();
        List<String> errors = new LinkedList<>();
        List<String> passed = new LinkedList<>();

        if (responseJson instanceof ArrayNode) {
            ArrayNode events = (ArrayNode) responseJson;

            for (int i = 0; i < events.size(); i++) {
                JsonNode event = events.get(i);

                if (event.has("eventId")) {

                    if (event.has("shouldRetry")) {

                        if ("true".equalsIgnoreCase(event.get("shouldRetry").asText())) {
                            retries.add(event.get("eventId").textValue());
                            continue;
                        }
                    }

                    if (event.has("error")) {
                        JsonNode jsonError = event.get("error");

                        if(jsonError instanceof ArrayNode) {
                            ArrayNode jsonErrors = (ArrayNode) jsonError;

                            if (jsonErrors.size() > 0) {
                                // one failed
                                errors.add(event.get("eventId").textValue());
                                continue;
                            }
                        }
                        else if(jsonError.has("message") && !jsonError.get("message").isNull()) {
                            // one failed
                            errors.add(event.get("eventId").textValue());
                            continue;
                        }
                    }
                    // one succeeded
                    passed.add(event.get("eventId").textValue());
                }
            }
        }
        assertTrue(passed.contains("D90C68D3-4E1F-416C-AA5E-DF44BF552691"));
        assertTrue(errors.contains("A5AEFEA1-4F09-4072-82E4-715A35D8733A"));
        assertTrue(retries.contains("D90C68D3-4E1F-416C-AA5E-DF44BF552690"));
    }

    private final static String ANDROID_PURCHASE_NOTIFICATION = "{\"version\":\"1.0\",\"packageName\":\"com.package\"," +
            "\"eventTimeMillis\":\"1600362712448\",\"subscriptionNotification\":{\"version\":\"1.0\",\"notificationType\":2," +
            "\"purchaseToken\":\"ljenggepalaaggfnimnhadkc.AO-J1OzgUXiI3xmMb9LyT-KKMUXVcffgIbTsIRMErdizxjCBlzI_0SSfPeskwLyq1xT5Ya3hGKTCSu8zY15TY6_NUdndMHTD10WVPXDfnMRhSf-yKbVC_NrFVwIxJa1fViyhzhQKv2XL2xIu99C4X6zW-EV6yO3WAQ\"," +
            "\"subscriptionId\":\"com.iap.renewing.1year.1\"}}";

    @Test
    public void testAndroidPurchaseNotification() throws Exception {
        JsonNode event = mapper.readTree(ANDROID_PURCHASE_NOTIFICATION);

        String data = mapper.writeValueAsString(event);
        String encodedData = Base64.getEncoder().encodeToString(data.getBytes());
        ObjectNode message = mapper.createObjectNode();
        message.put("data", encodedData);
        ObjectNode wrapper = mapper.createObjectNode();
        wrapper.set("message", message);

        System.out.println(mapper.writeValueAsString(wrapper));
    }

    private void processRecord(
            final CloningConsumer consumer,
            final ConsumerRecord<String, JsonNode> record,
            final List<ConsumerRecord<String, JsonNode>> noHeadersBatch,
            final List<ConsumerRecord<String, JsonNode>> headersBatch) {

        try {
            Method processRecord = CloningConsumer.class.getDeclaredMethod("processRecord", ConsumerRecord.class, List.class, List.class);
            processRecord.setAccessible(true);
            processRecord.invoke(consumer, record, noHeadersBatch, headersBatch);
        } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    private ConsumerRecord<String, JsonNode> newRecord(int i) {
        return newRecord(i, "user-attributes", Collections.singletonMap("pushToken", UUID.randomUUID().toString()), null);
    }

    private ConsumerRecord<String, JsonNode> newRecord(int i, String name, Map<String, String> attributes, Map<String, String> customDims) {
        ObjectNode value = mapper.createObjectNode();
        value.put("userId", UUID.randomUUID().toString());
        value.put("sessionId", UUID.randomUUID().toString());
        value.put("name", name);

        if(attributes != null) {
            ObjectNode attributesNode = value.putObject("attributes");
            attributes.forEach((k, v) -> attributesNode.put(k, v));
        }

        if(customDims != null) {
            ObjectNode customDimsNode = value.putObject("customDimensions");
            customDims.forEach((k, v) -> customDimsNode.put(k, v));
        }
        ConsumerRecord<String, JsonNode> record =
                new ConsumerRecord<>("test-topic", i, 100+i, String.valueOf(1000+i), value);
        return record;
    }

    private void validateObfuscated(JsonNode event) {
        assertTrue(event.get("userId").asText().startsWith(PREFIX));
        assertTrue(event.get("sessionId").asText().startsWith(PREFIX));
        JsonNode attrs = event.get("attributes");
        assertTrue(attrs.get("pushToken").asText().startsWith(PREFIX));
    }

    private void validateBatchConversion(CloningConsumer consumer, List<ConsumerRecord<String, JsonNode>> batch) throws Exception {
        List<JsonNode> expected =
                batch.stream()
                        .map(ConsumerRecord::value)
                        .collect(Collectors.toList());
        Method batchToJson = CloningConsumer.class.getDeclaredMethod("batchToJson", List.class);
        batchToJson.setAccessible(true);
        JsonNode node = (JsonNode)batchToJson.invoke(consumer, batch);
        ArrayNode events = (ArrayNode)node.get("events");
        List<JsonNode> actual = new LinkedList<>();
        events.elements().forEachRemaining(actual::add);
        assertEquals(expected.size(), actual.size());
    }
}
