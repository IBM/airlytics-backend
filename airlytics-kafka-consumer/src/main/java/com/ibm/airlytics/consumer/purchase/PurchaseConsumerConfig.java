package com.ibm.airlytics.consumer.purchase;

import com.amazonaws.thirdparty.jackson.annotation.JsonIgnoreProperties;
import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.AirlyticsConsumerConfig;

import javax.annotation.CheckForNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public class PurchaseConsumerConfig extends AirlyticsConsumerConfig {
    private static final String USERDB_PASSWORD = "USERDB_PASSWORD";

    private String eventApiBaseUrl;
    private String eventApiPath;
    private String eventProxyApiKey;
    private String eventProxyDevApiKey;
    private String purchasesTopic;
    private String notificationsTopic;
    private String dbUrl;
    private String dbUsername;
    private String purchasesTable;
    private String purchasesDevTable;
    private String purchasesUsersTable;
    private String purchasesUsersDevTable;
    private String purchasesEventsTable;
    private String purchasesEventsDevTable;
    private long refreshInAppProductsListInterval;
    private long refreshInAppProductsLisInitDelay;
    private long updatePurchaseRenewalStatusInterval;
    private long updatePurchaseRenewalStatusInitDelay;
    private long updatePurchaseRenewalStatusShortInterval;
    private long fullPurchaseUpdateInterval;
    private long fullPurchaseUpdateInitDelay;
    private long maxSubscriptionEventAge;
    private String airlockProductId;
    private int shards;
    private boolean subscriptionEventsEnabled = true;
    private boolean fixingBuggyRows = false;
    private String injectablePurchaseDataSource = null;

    @JsonIgnoreProperties(ignoreUnknown = true)
    private String s3Bucket;
    @JsonIgnoreProperties(ignoreUnknown = true)
    private String s3region;


    private boolean useSSL;
    private List<String> sqlStatesContinuationPrefixes = new ArrayList<>();

    private int userCacheSizeInRecords = 10000;

    public long getRefreshInAppProductsLisInitDelay() {
        return refreshInAppProductsLisInitDelay;
    }

    public void setRefreshInAppProductsLisInitDelay(long refreshInAppProductsLisInitDelay) {
        this.refreshInAppProductsLisInitDelay = refreshInAppProductsLisInitDelay;
    }

    public String getInjectablePurchaseDataSource() {
        return injectablePurchaseDataSource;
    }

    public void setInjectablePurchaseDataSource(String injectablePurchaseDataSource) {
        this.injectablePurchaseDataSource = injectablePurchaseDataSource;
    }

    public boolean isFixingBuggyRows() {
        return fixingBuggyRows;
    }

    public void setFixingBuggyRows(boolean fixingBuggyRows) {
        this.fixingBuggyRows = fixingBuggyRows;
    }

    public String getPurchasesEventsTable() {
        return purchasesEventsTable;
    }

    public void setPurchasesEventsTable(String purchasesEventsTable) {
        this.purchasesEventsTable = purchasesEventsTable;
    }

    public String getPurchasesEventsDevTable() {
        return purchasesEventsDevTable;
    }

    public void setPurchasesEventsDevTable(String purchasesEventsDevTable) {
        this.purchasesEventsDevTable = purchasesEventsDevTable;
    }

    public long getUpdatePurchaseRenewalStatusShortInterval() {
        return updatePurchaseRenewalStatusShortInterval;
    }

    public void setUpdatePurchaseRenewalStatusShortInterval(long updatePurchaseRenewalStatusShortInterval) {
        this.updatePurchaseRenewalStatusShortInterval = updatePurchaseRenewalStatusShortInterval;
    }

    public String getEventProxyDevApiKey() {
        return eventProxyDevApiKey;
    }

    public void setEventProxyDevApiKey(String eventProxyDevApiKey) {
        this.eventProxyDevApiKey = eventProxyDevApiKey;
    }

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.PURCHASE_ATTRIBUTE_CONSUMER);
    }

    public boolean isSubscriptionEventsEnabled() {
        return subscriptionEventsEnabled;
    }

    public void setSubscriptionEventsEnabled(boolean subscriptionEventsEnabled) {
        this.subscriptionEventsEnabled = subscriptionEventsEnabled;
    }

    public String getDbUrl() {
        return dbUrl;
    }

    public void setDbUrl(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    public String getDbUsername() {
        return dbUsername;
    }

    protected void setDbUsername(String dbUsername) {
        this.dbUsername = dbUsername;
    }

    public String getS3Bucket() {
        return s3Bucket;
    }

    public void setS3Bucket(String s3Bucket) {
        this.s3Bucket = s3Bucket;
    }

    public String getS3region() {
        return s3region;
    }

    public void setS3region(String s3region) {
        this.s3region = s3region;
    }

    @Deprecated
    public void setTopic(String topic) {
        //do nothing
    }

    @CheckForNull
    public String getDbPassword() {
        return System.getenv(USERDB_PASSWORD);
    }

    public String getPurchasesTable() {
        return purchasesTable;
    }

    public void setPurchasesTable(String purchasesTable) {
        this.purchasesTable = purchasesTable;
    }

    public long getRefreshInAppProductsListInterval() {
        return refreshInAppProductsListInterval;
    }

    public void setRefreshInAppProductsListInterval(long refreshInAppProductsListInterval) {
        this.refreshInAppProductsListInterval = refreshInAppProductsListInterval;
    }

    public boolean isUseSSL() {
        return useSSL;
    }

    protected void setUseSSL(boolean useSSL) {
        this.useSSL = useSSL;
    }

    List<String> getSqlStatesContinuationPrefixes() {
        return sqlStatesContinuationPrefixes;
    }

    protected void setSqlStatesContinuationPrefixes(List<String> sqlStatesContinuationPrefixes) {
        this.sqlStatesContinuationPrefixes = sqlStatesContinuationPrefixes;
    }


    public String getAirlockProductId() {
        return airlockProductId;
    }

    public void setAirlockProductId(String airlockProductId) {
        this.airlockProductId = airlockProductId;
    }

    public int getUserCacheSizeInRecords() {
        return userCacheSizeInRecords;
    }

    public void setUserCacheSizeInRecords(int userCacheSizeInRecords) {
        this.userCacheSizeInRecords = userCacheSizeInRecords;
    }

    public String getPurchasesDevTable() {
        return purchasesDevTable;
    }

    public void setPurchasesDevTable(String purchasesDevTable) {
        this.purchasesDevTable = purchasesDevTable;
    }

    public String getPurchasesUsersDevTable() {
        return purchasesUsersDevTable;
    }

    public void setPurchasesUsersDevTable(String purchasesUsersDevTable) {
        this.purchasesUsersDevTable = purchasesUsersDevTable;
    }

    public String getEventApiBaseUrl() {
        return eventApiBaseUrl;
    }

    public void setEventApiBaseUrl(String eventApiBaseUrl) {
        this.eventApiBaseUrl = eventApiBaseUrl;
    }

    public String getEventApiPath() {
        return eventApiPath;
    }

    public void setEventApiPath(String eventApiPath) {
        this.eventApiPath = eventApiPath;
    }

    public String getEventProxyApiKey() {
        return eventProxyApiKey;
    }

    public String getPurchasesTopic() {
        return purchasesTopic;
    }

    public void setPurchasesTopic(String purchasesTopic) {
        this.purchasesTopic = purchasesTopic;
    }

    public String getNotificationsTopic() {
        return notificationsTopic;
    }

    public void setNotificationsTopic(String notificationsTopic) {
        this.notificationsTopic = notificationsTopic;
    }

    public void setEventProxyApiKey(String eventProxyApiKey) {
        this.eventProxyApiKey = eventProxyApiKey;
    }

    public static String getUserdbPassword() {
        return USERDB_PASSWORD;
    }

    public long getFullPurchaseUpdateInterval() {
        return fullPurchaseUpdateInterval;
    }

    public void setFullPurchaseUpdateInterval(long fullPurchaseUpdateInterval) {
        this.fullPurchaseUpdateInterval = fullPurchaseUpdateInterval;
    }

    public long getFullPurchaseUpdateInitDelay() {
        return fullPurchaseUpdateInitDelay;
    }

    public void setFullPurchaseUpdateInitDelay(long fullPurchaseUpdateInitDelay) {
        this.fullPurchaseUpdateInitDelay = fullPurchaseUpdateInitDelay;
    }

    public long getUpdatePurchaseRenewalStatusInterval() {
        return updatePurchaseRenewalStatusInterval;
    }

    public void setUpdatePurchaseRenewalStatusInterval(long updatePurchaseRenewalStatusInterval) {
        this.updatePurchaseRenewalStatusInterval = updatePurchaseRenewalStatusInterval;
    }

    public long getUpdatePurchaseRenewalStatusInitDelay() {
        return updatePurchaseRenewalStatusInitDelay;
    }

    public void setUpdatePurchaseRenewalStatusInitDelay(long updatePurchaseRenewalStatusInitDelay) {
        this.updatePurchaseRenewalStatusInitDelay = updatePurchaseRenewalStatusInitDelay;
    }

    public long getMaxSubscriptionEventAge() {
        return maxSubscriptionEventAge;
    }

    public void setMaxSubscriptionEventAge(long maxSubscriptionEventAge) {
        this.maxSubscriptionEventAge = maxSubscriptionEventAge;
    }

    public int getShards() {
        return shards;
    }

    public void setShards(int shards) {
        this.shards = shards;
    }

    public String getPurchasesUsersTable() {
        return purchasesUsersTable;
    }

    public void setPurchasesUsersTable(String purchasesUsersTable) {
        this.purchasesUsersTable = purchasesUsersTable;
    }
}
