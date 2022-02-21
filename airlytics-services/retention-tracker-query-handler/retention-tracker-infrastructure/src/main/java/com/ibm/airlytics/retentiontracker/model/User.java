package com.ibm.airlytics.retentiontracker.model;

public class User {
    private String userId;
    private String pushToken;
    private String platform;
    private String productId;

    public User(String userId, String pushToken, String platform, String productId) {
        this.userId = userId;
        this.pushToken = pushToken;
        this.platform = platform;
        this.productId = productId;
    }

    public String getUserId() {
        return userId;
    }

    public String getPushToken() {
        return pushToken;
    }

    public String getPlatform() {
        return platform;
    }

    public String getProductId() {
        return productId;
    }
}
