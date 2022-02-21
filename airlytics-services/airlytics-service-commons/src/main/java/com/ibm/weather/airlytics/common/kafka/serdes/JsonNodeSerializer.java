package com.ibm.weather.airlytics.common.kafka.serdes;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class JsonNodeSerializer implements Serializer<JsonNode> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public JsonNodeSerializer() {

    }

    @Override
    public byte[] serialize(String topic, JsonNode data) {

        if (data == null) {
            return null;
        }

        try {
            byte[] bytes = objectMapper.writeValueAsBytes(data);
            return bytes;
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {

    }
}
