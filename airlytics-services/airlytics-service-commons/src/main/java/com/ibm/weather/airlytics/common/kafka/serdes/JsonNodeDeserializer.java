package com.ibm.weather.airlytics.common.kafka.serdes;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class JsonNodeDeserializer implements Deserializer<JsonNode> {
    private ObjectMapper objectMapper = new ObjectMapper();

    public JsonNodeDeserializer() {

    }

    @Override
    public JsonNode deserialize(String topic, byte[] data) {

        if (data == null) {
            return null;
        }

        try {
            return objectMapper.readTree(data);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public void close() {
        //nothing to close
    }
}
