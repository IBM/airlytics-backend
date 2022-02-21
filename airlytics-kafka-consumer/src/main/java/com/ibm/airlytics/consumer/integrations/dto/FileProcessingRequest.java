package com.ibm.airlytics.consumer.integrations.dto;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class FileProcessingRequest {
    private String fileName;
    private String bucketName;

    public FileProcessingRequest(ConsumerRecord<String, JsonNode> record) throws UnsupportedEncodingException {
        JsonNode event = record.value();
        String encodedFileName = event.get("fileName") == null ? null : event.get("fileName").asText();
        if (encodedFileName != null) {
            fileName = URLDecoder.decode(encodedFileName, "UTF-8");
        }
        bucketName = event.get("bucketName") == null ? null : event.get("bucketName").asText();
    }

    public FileProcessingRequest(String bucketName, String fileName) {
        this.bucketName = bucketName;
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }

    public String getBucketName() {
        return bucketName;
    }
}
