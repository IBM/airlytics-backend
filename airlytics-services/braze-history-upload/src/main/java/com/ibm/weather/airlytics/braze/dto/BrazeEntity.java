package com.ibm.weather.airlytics.braze.dto;

import java.util.LinkedHashMap;
import java.util.Map;

/** Represents a Braze Tracking Event, or User Attributes, or InstallOrPurchase object */
public class BrazeEntity {

    public static enum Kind {EVENT, USER, PURCHASE}

    private Kind kind;

    private String external_id;

    private String app_id;

    private String name;// for events

    private String product_id;// for purchases

    private String currency;// for purchases

    private Float price;// for purchases

    private String time;// for events & purchases

    private Map<String, Object> properties;

    public Kind getKind() {

        if(kind == null) {
            if (name == null && time == null) return Kind.USER;
            else if (product_id == null && currency == null && price == null) return Kind.EVENT;
            else return Kind.PURCHASE;
        }
        return kind;
    }

    public void setKind(Kind kind) {
        this.kind = kind;
    }

    public String getExternal_id() {
        return external_id;
    }

    public void setExternal_id(String external_id) {
        this.external_id = external_id;
    }

    public String getApp_id() {
        return app_id;
    }

    public void setApp_id(String app_id) {
        this.app_id = app_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getProduct_id() {
        return product_id;
    }

    public void setProduct_id(String product_id) {
        this.product_id = product_id;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public Float getPrice() {
        return price;
    }

    public void setPrice(Float price) {
        this.price = price;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> result = new LinkedHashMap<>();
        Kind kind = getKind();

        if(kind == Kind.USER) { // User Attributes Object
            result.put("external_id", external_id);

            if (properties != null) result.putAll(properties);
        } else if(kind == Kind.EVENT) { // Event Object
            result.put("app_id", app_id);
            result.put("external_id", external_id);
            result.put("time", time);
            result.put("name", name);
            result.put("properties", properties);
        } else if(kind == Kind.PURCHASE) { // InstallOrPurchase
            result.put("app_id", app_id);
            result.put("external_id", external_id);
            result.put("time", time);
            result.put("product_id", product_id);
            result.put("currency", currency);
            result.put("price", price);
            result.put("properties", properties);
        }
        return result;
    }
}
