package com.ibm.weather.airlytics.amplitude.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class AmplitudeEvent {

    public static final String AMP_TYPE_IDENTIFY = "$identify";
    public static final String AMP_TYPE_DUMMY = "history-uploaded";

    private String user_id; // A readable ID specified by you. Must have a minimum length of 5 characters. Required unless device_id is present.
    private String device_id; // A device-specific identifier, such as the Identifier for Vendor on iOS. Required unless user_id is present.
    // If a device_id is not sent with the event, it will be set to a hashed version of the user_id.
    private String event_type; // A unique identifier for your event.
    private long time; // The timestamp of the event in milliseconds since epoch. If time is not sent with the event,
    // it will be set to the request upload time.
    @JsonInclude(value=JsonInclude.Include.NON_NULL, content = JsonInclude.Include.ALWAYS)
    private Map<String, Object> event_properties; // A dictionary of key-value pairs that represent additional data to be sent along with the event.
    // You can store property values in an array. Date values are transformed into String values.
    // Object depth may not exceed 40 layers.
    @JsonInclude(value=JsonInclude.Include.NON_NULL, content = JsonInclude.Include.ALWAYS)
    private Map<String, Object> user_properties; // A dictionary of key-value pairs that represent additional data tied to the user.
    // You can store property values in an array. Date values are transformed into String values.
    // Object depth may not exceed 40 layers.
    private Map<String, String> groups; // This feature is only available to Enterprise customers who have purchased the Accounts add-on.
    // This field adds a dictionary of key-value pairs that represent groups of users to the event
    // as an event-level group. You can only track up to 5 groups.
    private String app_version; // The current version of your application.
    private String platform; // Platform of the device.
    private String os_name; // The name of the mobile operating system or browser that the user is using.
    private String os_version; // The version of the mobile operating system or browser the user is using.
    private String device_brand; // The device brand that the user is using.
    private String device_manufacturer; // The device manufacturer that the user is using.
    private String device_model; // The device model that the user is using.
    private String carrier; // The carrier that the user is using.
    private String country; // The current country of the user.
    private String region; // The current region of the user.
    private String city; // The current city of the user.
    private String dma; // The current Designated Market Area of the user.
    private String language; // The language set by the user.
    private Float price; // The price of the item purchased. Required for revenue data if the revenue field is not sent.
    // You can use negative values to indicate refunds.
    private Integer quantity; // The quantity of the item purchased. Defaults to 1 if not specified.
    private Float revenue; // revneue = price quantity. If you send all 3 fields of price, quantity, and revenue,
    // then (price quantity) will be used as the revenue value. You can use negative values to indicate refunds.
    private String productId; // An identifier for the item purchased. You must send a price and quantity or revenue with this field.
    private String revenueType; // The type of revenue for the item purchased. You must send a price and quantity or revenue with this field.
    private Float location_lat; // The current Latitude of the user.
    private Float location_lng; // The current Longitude of the user.
    private String ip; // The IP address of the user. Use ""$remote"" to use the IP address on the upload request.
    // We will use the IP address to reverse lookup a user's location (city, country, region, and DMA).
    // Amplitude has the ability to drop the location and IP address from events once it reaches our servers.
    // You can submit a request to our platform specialist team here to configure this for you.
    private String idfa; // (iOS) Identifier for Advertiser.
    private String idfv; // (iOS) Identifier for Vendor.
    private String adid; // (Android) Google Play Services advertising ID
    private String android_id; // (Android) Android ID (not the advertising ID)
    private Integer event_id; // (Optional) An incrementing counter to distinguish events with the same user_id and timestamp from each other.
    // We recommend you send an event_id, increasing over time, especially if you expect events to occur simultanenously.
    private long session_id; // (Optional) The start time of the session in milliseconds since epoch (Unix Timestamp),
    // necessary if you want to associate events with a particular system. A session_id of -1 is the same as no session_id specified.
    private String insert_id; // (Optional) A unique identifier for the event.
    // We will deduplicate subsequent events sent with an insert_id we have already seen before within the past 7 days.
    // We recommend generation a UUID or using some combination of device_id, user_id, event_type, event_id, and time.

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getDevice_id() {
        return device_id;
    }

    public void setDevice_id(String device_id) {
        this.device_id = device_id;
    }

    public String getEvent_type() {
        return event_type;
    }

    public void setEvent_type(String event_type) {
        this.event_type = event_type;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public Map<String, Object> getEvent_properties() {
        return event_properties;
    }

    public void setEvent_properties(Map<String, Object> event_properties) {
        this.event_properties = event_properties;
    }

    public Map<String, Object> getUser_properties() {
        return user_properties;
    }

    public void setUser_properties(Map<String, Object> user_properties) {
        this.user_properties = user_properties;
    }

    public Map<String, String> getGroups() {
        return groups;
    }

    public void setGroups(Map<String, String> groups) {
        this.groups = groups;
    }

    public String getApp_version() {
        return app_version;
    }

    public void setApp_version(String app_version) {
        this.app_version = app_version;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getOs_name() {
        return os_name;
    }

    public void setOs_name(String os_name) {
        this.os_name = os_name;
    }

    public String getOs_version() {
        return os_version;
    }

    public void setOs_version(String os_version) {
        this.os_version = os_version;
    }

    public String getDevice_brand() {
        return device_brand;
    }

    public void setDevice_brand(String device_brand) {
        this.device_brand = device_brand;
    }

    public String getDevice_manufacturer() {
        return device_manufacturer;
    }

    public void setDevice_manufacturer(String device_manufacturer) {
        this.device_manufacturer = device_manufacturer;
    }

    public String getDevice_model() {
        return device_model;
    }

    public void setDevice_model(String device_model) {
        this.device_model = device_model;
    }

    public String getCarrier() {
        return carrier;
    }

    public void setCarrier(String carrier) {
        this.carrier = carrier;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getDma() {
        return dma;
    }

    public void setDma(String dma) {
        this.dma = dma;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public Float getPrice() {
        return price;
    }

    public void setPrice(Float price) {
        this.price = price;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
    }

    public Float getRevenue() {
        return revenue;
    }

    public void setRevenue(Float revenue) {
        this.revenue = revenue;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getRevenueType() {
        return revenueType;
    }

    public void setRevenueType(String revenueType) {
        this.revenueType = revenueType;
    }

    public Float getLocation_lat() {
        return location_lat;
    }

    public void setLocation_lat(Float location_lat) {
        this.location_lat = location_lat;
    }

    public Float getLocation_lng() {
        return location_lng;
    }

    public void setLocation_lng(Float location_lng) {
        this.location_lng = location_lng;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getIdfa() {
        return idfa;
    }

    public void setIdfa(String idfa) {
        this.idfa = idfa;
    }

    public String getIdfv() {
        return idfv;
    }

    public void setIdfv(String idfv) {
        this.idfv = idfv;
    }

    public String getAdid() {
        return adid;
    }

    public void setAdid(String adid) {
        this.adid = adid;
    }

    public String getAndroid_id() {
        return android_id;
    }

    public void setAndroid_id(String android_id) {
        this.android_id = android_id;
    }

    public Integer getEvent_id() {
        return event_id;
    }

    public void setEvent_id(Integer event_id) {
        this.event_id = event_id;
    }

    public long getSession_id() {
        return session_id;
    }

    public void setSession_id(long session_id) {
        this.session_id = session_id;
    }

    public String getInsert_id() {
        return insert_id;
    }

    public void setInsert_id(String insert_id) {
        this.insert_id = insert_id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AmplitudeEvent that = (AmplitudeEvent) o;

        return insert_id != null ? insert_id.equals(that.insert_id) : that.insert_id == null;
    }

    @Override
    public int hashCode() {
        return insert_id != null ? insert_id.hashCode() : 0;
    }
}
