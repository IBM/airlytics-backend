package com.ibm.airlytics.consumer.dsr;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Timestamp;
import java.util.Iterator;

public class DSRRequest {
    public static final String REQUEST_TYPE_PORTABILITY="Portability";
    public static final String REQUEST_TYPE_DELETE="Delete";

    private String requestType;
    private String product;
    private String requestS3Path;
    private String upsId;
    private String airlyticsId;
    private String requestId;
    private String dayPath;

    public DSRRequest(ConsumerRecord<String, JsonNode> record) {
        JsonNode event = record.value();
        if (event.get("rType")!=null) {
            requestType = event.get("rType").asText();
        }
        product = event.get("appId").asText();
        requestId = event.get("requestId").asText();
        dayPath = event.get("dayPath").asText();
        //requestS3Path = event.get("requestS3Path").asText();

        JsonNode partners = event.get("partners");
        Iterator<JsonNode> partnerIterator = partners.elements();
        while (partnerIterator.hasNext()) {
            JsonNode partner = partnerIterator.next();
            if (partner.get("Name").asText().equals("UPS") ||
                    partner.get("Name").asText().equals("Services_Data")) {
                upsId = partner.get("ID").asText();
            } else if (partner.get("Name").asText().equals("airlytics")) {
                airlyticsId = partner.get("ID").asText();
            }
        }
    }

    public String getRequestType() {
        return requestType;
    }

    public String getProduct() {
        return product;
    }

    public String getRequestS3Path() {
        return requestS3Path;
    }

    public String getUpsId() {
        return upsId;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getDayPath() {
        return dayPath;
    }

    public String getAirlyticsId() {
        return airlyticsId;
    }
}
