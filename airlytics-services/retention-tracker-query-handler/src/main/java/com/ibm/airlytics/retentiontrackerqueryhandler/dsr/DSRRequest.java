package com.ibm.airlytics.retentiontrackerqueryhandler.dsr;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Iterator;

public class DSRRequest {
    public static final String REQUEST_TYPE_PORTABILITY="Portability";
    public static final String REQUEST_TYPE_DELETE="Delete";

    private String requestType;
    private String product;
    private String requestS3Path;
    private String upsId;

    public DSRRequest(JSONObject event) {
        requestType = event.getString("rType");
        product = event.getString("appId");
        //requestS3Path = event.get("requestS3Path").asText();

        JSONArray partners = event.getJSONArray("partners");
        int length = partners.length();

        for (int i=0; i<length; ++i) {
            JSONObject partner = partners.getJSONObject(i);
            if (partner.getString("Name").equals("UPS")) {
                upsId = partner.getString("ID");
                break;
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
}
