package com.ibm.weather.airlytics.common.kafka.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public abstract class JsonSerializer<T> implements Serializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public JsonSerializer() {

    }

    @Override
    public byte[] serialize(String topic, T data) {

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
