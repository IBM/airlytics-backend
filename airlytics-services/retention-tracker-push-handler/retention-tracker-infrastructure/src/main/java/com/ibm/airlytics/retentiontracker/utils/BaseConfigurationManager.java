package com.ibm.airlytics.retentiontracker.utils;

import com.ibm.airlock.common.data.Feature;
import com.ibm.airlytics.retentiontracker.airlock.AirlockConstants;
import com.ibm.airlytics.retentiontracker.airlock.AirlockManager;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import com.amazonaws.services.secretsmanager.*;
import com.amazonaws.services.secretsmanager.model.*;
import org.json.JSONObject;
import java.util.Base64;

public class BaseConfigurationManager {
    private static final String AIRLYTICS_ENVIRONMENT_ENV_VARIABLE = "AIRLYTICS_ENVIRONMENT";
    private static RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(BaseConfigurationManager.class.getName());

    private String dbUrl;
    private String dbUser;
    private String dbPass;
    private String dbUrlRO;
    private String dbUserRO;
    private String dbPassRO;
    private String rabbitmqUri;
    private String apnsProductId;
    private String apnsCertPass;
    private String apnsCert;
    private String apnsKey;
    private String apnsKeyId;
    private String apnsTeamId;
    private String airlockApiKey;
    private String airlockApiPassword;
    private String fcmDbUrl;
    private String fcmConfig;
    private String iosQueueName;
    private String androidQueueName;
    private String bouncedQueueName;
    private String publisherQueueName;
    private String listenerQueueName;
    private String exchangeName;
    private long batchSize;
    private int pushThreads;
    private int inactivateThreads;
    private JSONObject platformIds;
    private JSONObject proxyPlatformKeys;
    private JSONObject kafkaTopics;
    private int feedbackServiceInterval;
    private long feedbackServiceNumRetries;
    private boolean useAPNSProdServer;
    private long trackerIosInitialDelay;
    private long trackerAndroidInitialDelay;
    private long trackerIosDelay;
    private long trackerAndroidDelay;

    private long trackerInitialDelay;
    private long trackerDelay;

    private String athenaRegion;
    private String athenaOutputBucket;
    private String athenaCatalog;
    private String athenaDB;

    private String dbExporterRegion;
    private String dbExporterIamRole;
    private String dbExporterS3BucketName;
    private String dbExporterSourceArn;
    private String dbExporterKmsKeyId;
    private String dbExporterS3Prefix;
    private String dbExporterIdentifier;
    private String dbExporterDbInstanceIdentifier;
    private String dbExporterSnapshotPrefix;

    private long pollJobInitialDelay;
    private long pollJobdelay;

    private String airLockSDKBaseURL;
    private String airLockInternalProductId;
    private Boolean airLockServerAuthentication;

    public String getDbExporterS3Prefix() {
        return dbExporterS3Prefix;
    }

    public String getDbExporterRegion() {
        return dbExporterRegion;
    }

    public String getDbExporterIamRole() {
        return dbExporterIamRole;
    }

    public String getDbExporterS3BucketName() {
        return dbExporterS3BucketName;
    }

    public String getDbExporterSourceArn() {
        return dbExporterSourceArn;
    }

    public String getDbExporterKmsKeyId() {
        return dbExporterKmsKeyId;
    }

    public int getDbConnectionMinPoolSize() {
        return dbConnectionMinPoolSize;
    }

    public int getDbConnectionInitialPoolSize() {
        return dbConnectionInitialPoolSize;
    }

    public int getDbConnectionMaxPoolSize() {
        return dbConnectionMaxPoolSize;
    }

    public int getDbConnectionAcquireIncrement() {
        return dbConnectionAcquireIncrement;
    }
    public static String getEnvVar() {
        return Environment.getAlphanumericEnv(AIRLYTICS_ENVIRONMENT_ENV_VARIABLE, false).toLowerCase();
    }

    public long getPollJobInitialDelay() { return pollJobInitialDelay; }

    public long getPollJobdelay() {
        return pollJobdelay;
    }

    public String getAirlockBaseURL() { return airLockSDKBaseURL; }
    public String getAirLockInternalProductId() { return airLockInternalProductId; }
    public Boolean getAirLockServerAuthentication() { return  airLockServerAuthentication; }

    private int dbConnectionMinPoolSize;
    private int dbConnectionInitialPoolSize;
    private int dbConnectionMaxPoolSize;
    private int dbConnectionAcquireIncrement;

    public String getEventsProxyBaseURL() {
        return eventsProxyBaseURL;
    }

    public String getEventsProxyTrackPath() {
        return eventsProxyTrackPath;
    }

    public String getAirlockApiKey() { return airlockApiKey; }

    public String getAirlockApiPassword() {return airlockApiPassword; }

    public String getApnsKey() {
        return apnsKey;
    }

    public String getApnsKeyId() {
        return apnsKeyId;
    }

    public String getApnsTeamId() {
        return apnsTeamId;
    }


    private String eventsProxyBaseURL;
    private String eventsProxyTrackPath;
    private int eventApiRateLimit;
    private int eventApiBatchSize = 1;

    private int eventApiRetries = 3;


    public BaseConfigurationManager() {
        populateQueueData();
        populateConfigurations();
        populatePlatformIds();
        populateEventsProxyCredentials();
        populateKafkaTopics();
        populatePushHandlerConfigs();
        populateDbConnectionConfigs();
        populateTrackerTimingConfigs();
    }

    private void populateTrackerTimingConfigs() {
        Feature feature = AirlockManager.getInstance().getFeature(AirlockConstants.infrastracture.TRACKER_TIMING_CONFIGURATIONS);
        trackerIosInitialDelay = feature.getConfiguration().getLong("iosInitialDelayMinutes");
        trackerAndroidInitialDelay = feature.getConfiguration().getLong("androidInitialDelayMinutes");
        trackerIosDelay = feature.getConfiguration().getLong("iosDelayMinutes");
        trackerAndroidDelay = feature.getConfiguration().getLong("androidDelayMinutes");
        trackerInitialDelay = feature.getConfiguration().getLong("initialDelayMinutes");
        trackerDelay = feature.getConfiguration().getLong("delayMinutes");
    }

    private void populateDbConnectionConfigs() {
        Feature feature = AirlockManager.getInstance().getFeature(AirlockConstants.infrastracture.DB_CONNECTION_CONFIGURATIONS);
        dbConnectionMinPoolSize = feature.getConfiguration().getInt("minPoolSize");
        dbConnectionMaxPoolSize = feature.getConfiguration().getInt("maxPoolSize");
        dbConnectionAcquireIncrement = feature.getConfiguration().getInt("acquireIncrement");
        dbConnectionInitialPoolSize = feature.getConfiguration().getInt("initialPoolSize");
    }

    protected void populateAthenaConfigs() {
        Feature feature = AirlockManager.getInstance().getFeature(AirlockConstants.infrastracture.ATHENA_HANDLER_CONFIGURATIONS);
        athenaRegion = feature.getConfiguration().getString("region");
        athenaOutputBucket = feature.getConfiguration().getString("outputBucket");
        athenaCatalog = feature.getConfiguration().getString("catalog");
        athenaDB = feature.getConfiguration().getString("athenaDB");
    }

    protected void populateDbExporterConfigs() {
        Feature feature = AirlockManager.getInstance().getFeature(AirlockConstants.infrastracture.RDS_DB_EXPORTER_CONFIGURATIONS);
        dbExporterRegion = feature.getConfiguration().getString("region");
        dbExporterIamRole = feature.getConfiguration().getString("iamRole");
        dbExporterS3BucketName = feature.getConfiguration().getString("s3BucketName");
        dbExporterSourceArn = feature.getConfiguration().getString("sourceArn");
        dbExporterKmsKeyId = feature.getConfiguration().getString("kmsKeyId");
        dbExporterS3Prefix = feature.getConfiguration().getString("s3Prefix");
        dbExporterIdentifier = feature.getConfiguration().getString("identifier");
        dbExporterDbInstanceIdentifier = feature.getConfiguration().getString("dbInstanceIdentifier");
        dbExporterSnapshotPrefix = feature.getConfiguration().getString("snapshotPrefix");
    }
    private void populatePushHandlerConfigs() {
        Feature feature = AirlockManager.getInstance().getFeature(AirlockConstants.infrastracture.PUSH_HANDLER_CONFIGURATIONS);
        feedbackServiceInterval = feature.getConfiguration().getInt("feedbackServiceIntervalMinutes");
        useAPNSProdServer = feature.getConfiguration().getBoolean("useAPNSProdServer");
        feedbackServiceNumRetries = feature.getConfiguration().getLong("feedbackServiceNumRetries");
    }

    private void populateKafkaTopics() {
        Feature feature = AirlockManager.getInstance().getFeature(AirlockConstants.infrastracture.KAFKA_CONFIGURATIONS);
        kafkaTopics = feature.getConfiguration().getJSONObject("topics");
    }

    protected void populateEventsProxyCredentials() {
        Feature feature = AirlockManager.getInstance().getFeature(AirlockConstants.infrastracture.EVENTS_PROXY_CONFIGURATION);
        proxyPlatformKeys = feature.getConfiguration().getJSONObject("api_keys");
        eventsProxyBaseURL = feature.getConfiguration().getString("base_url");
        eventsProxyTrackPath = feature.getConfiguration().getString("track_path");
        eventApiRetries = feature.getConfiguration().getInt("eventsProxyRetries");
        eventApiBatchSize = feature.getConfiguration().getInt("eventApiBatchSize");
        eventApiRateLimit = feature.getConfiguration().getInt("eventApiRateLimit");

    }
    protected void populatePlatformIds() {
        Feature feature = AirlockManager.getInstance().getFeature(AirlockConstants.infrastracture.PRODUCT_IDS);
        platformIds = feature.getConfiguration();

    }

    protected void populatePollsConfigs() {
        Feature feature = AirlockManager.getInstance().getFeature(AirlockConstants.infrastracture.POLLS_CONFIGURATION);

        JSONObject configuration = feature.getConfiguration();
        if (configuration == null) {
            return;
        }
        pollJobInitialDelay = configuration.getLong("initialDelay");
        pollJobdelay = configuration.getLong("delay");
    }

    protected void populateAirlockConfigs() {
        Feature feature = AirlockManager.getInstance().getFeature(AirlockConstants.infrastracture.AIRLOCK_API_CONFIGURATIONS);
        JSONObject configuration = feature.getConfiguration();
        if (configuration == null) {
            return;
        }
        airLockSDKBaseURL = configuration.getString("baseURL");
        airLockInternalProductId = configuration.getString("productId");
        airLockServerAuthentication = configuration.getBoolean("authentication");
    }

    public String getEventsApiKey(String platform) {
        return proxyPlatformKeys.getString(platform);
    }
    public String getProductId(String platform) {
        return platformIds.getString(platform);
    }
    protected void populateConfigurations() {
        batchSize = getLongValue("batchSize", AirlockConstants.infrastracture.CONFIGURATIONS);
        pushThreads = getIntValue("pushThreads", AirlockConstants.infrastracture.CONFIGURATIONS);
        inactivateThreads = getIntValue("inactivateThreads", AirlockConstants.infrastracture.CONFIGURATIONS);
    }

    protected void populateQueueData() {
        iosQueueName = getValue("iosQueueName", AirlockConstants.infrastracture.RABBITMQ_CONFIGURATION);
        androidQueueName = getValue("androidQueueName", AirlockConstants.infrastracture.RABBITMQ_CONFIGURATION);
        bouncedQueueName = getValue("bouncedQueueName", AirlockConstants.infrastracture.RABBITMQ_CONFIGURATION);
        publisherQueueName = getValue("publisherQueueName", AirlockConstants.infrastracture.RABBITMQ_CONFIGURATION);
        listenerQueueName = getValue("listenerQueueName", AirlockConstants.infrastracture.RABBITMQ_CONFIGURATION);
        exchangeName = getValue("exchangeName", AirlockConstants.infrastracture.RABBITMQ_CONFIGURATION);
    }

    protected String getValue(String value, String featureName) {
        Feature feature = AirlockManager.getInstance().getFeature(featureName);
        if (feature.isOn()) {
            return feature.getConfiguration().getString(value);
        }
        logger.warn("trying to access feature "+featureName+" which is off");
        return null;
    }

    protected long getLongValue(String value, String featureName) {
        Feature feature = AirlockManager.getInstance().getFeature(featureName);
        if (feature.isOn()) {
            return feature.getConfiguration().getLong(value);
        }
        logger.warn("trying to access feature "+featureName+" which is off");
        return 0;
    }

    protected int getIntValue(String value, String featureName) {
        Feature feature = AirlockManager.getInstance().getFeature(featureName);
        if (feature.isOn()) {
            return feature.getConfiguration().getInt(value);
        }
        logger.warn("trying to access feature "+featureName+" which is off");
        return 0;
    }

    protected JSONObject getJsonValue(String value, String featureName) {
        Feature feature = AirlockManager.getInstance().getFeature(featureName);
        if (feature.isOn()) {
            return feature.getConfiguration().getJSONObject(value);
        }
        logger.warn("trying to access feature "+featureName+" which is off");
        return null;
    }


    protected void populateDbCreds() {
        String dbType = System.getenv("DB_TYPE");
        if (dbType == null) {
            dbType = "DB";
        }
        JSONObject config = getJsonValue(dbType,AirlockConstants.infrastracture.SECRETS);
        String secretName = config.getString("name");
        String region = config.getString("region");
        JSONObject db = getCredsJSON(secretName, region);

        String host = db.getString("host");
        String port = db.getString("port");
        String dbName = db.getString("dbname");
        dbUrl = "jdbc:postgresql://"+host+":"+port+"/"+dbName;
        dbUser = db.getString("username");
        dbPass = db.getString("password");
    }

    protected void populateDbROCreds() {
        String dbType = "DB-RO";
        JSONObject config = getJsonValue(dbType,AirlockConstants.infrastracture.SECRETS);
        String secretName = config.getString("name");
        String region = config.getString("region");
        JSONObject db = getCredsJSON(secretName, region);

        String host = db.getString("host");
        String port = db.getString("port");
        String dbName = db.getString("dbname");
        dbUrlRO = "jdbc:postgresql://"+host+":"+port+"/"+dbName;
        dbUserRO = db.getString("username");
        dbPassRO = db.getString("password");
    }

    protected void populateRabbitCreds() {

        JSONObject config = getJsonValue("RABBITMQ",AirlockConstants.infrastracture.SECRETS);
        String secretName = config.getString("name");
        String region = config.getString("region");
        JSONObject obj = getCredsJSON(secretName, region);
        rabbitmqUri = obj.getString("uri");
    }

    protected void populateApnsCreds() {

        JSONObject config = getJsonValue("APNS",AirlockConstants.infrastracture.SECRETS);
        String secretName = config.getString("name");
        String region = config.getString("region");
        JSONObject obj = getCredsJSON(secretName, region);
        apnsCertPass = obj.getString("cert_pass");
        apnsProductId = obj.getString("app_id");
        apnsCert = obj.getString("cert");
        apnsKey = obj.getString("key");
        apnsKeyId = obj.getString("key_id");
        apnsTeamId = obj.getString("team_id");
    }

    protected void populateAirlockApiCreds() {

        JSONObject config = getJsonValue("AIRLOCK-API",AirlockConstants.infrastracture.SECRETS);
        String secretName = config.getString("name");
        String region = config.getString("region");
        JSONObject obj = getCredsJSON(secretName, region);
        airlockApiKey = obj.getString("key");
        airlockApiPassword = obj.getString("password");
    }

    protected  void populateFcmCreds() {

        JSONObject config = getJsonValue("FCM",AirlockConstants.infrastracture.SECRETS);
        String secretName = config.getString("name");
        String region = config.getString("region");
        JSONObject obj = getCredsJSON(secretName, region);
        fcmDbUrl = obj.getString("databaseurl");
        fcmConfig = obj.getString("configuration");
    }

    private static JSONObject getCredsJSON(String secretName, String region) {
        logger.info("get credentials for secret:"+secretName+" region:"+region);

        // Create a Secrets Manager client
        AWSSecretsManager client  = AWSSecretsManagerClientBuilder.standard()
                .withRegion(region)
                .build();

        // In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
        // See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        // We rethrow the exception by default.

        String secret = null, decodedBinarySecret;
        GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest()
                .withSecretId(secretName);
        GetSecretValueResult getSecretValueResult = null;

        try {
            getSecretValueResult = client.getSecretValue(getSecretValueRequest);
        } catch (DecryptionFailureException e) {
            // Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            // Deal with the exception here, and/or rethrow at your discretion.
            throw e;
        } catch (InternalServiceErrorException e) {
            // An error occurred on the server side.
            // Deal with the exception here, and/or rethrow at your discretion.
            throw e;
        } catch (InvalidParameterException e) {
            // You provided an invalid value for a parameter.
            // Deal with the exception here, and/or rethrow at your discretion.
            throw e;
        } catch (InvalidRequestException e) {
            // You provided a parameter value that is not valid for the current state of the resource.
            // Deal with the exception here, and/or rethrow at your discretion.
            throw e;
        } catch (ResourceNotFoundException e) {
            // We can't find the resource that you asked for.
            // Deal with the exception here, and/or rethrow at your discretion.
            throw e;
        }

        // Decrypts secret using the associated KMS CMK.
        // Depending on whether the secret is a string or binary, one of these fields will be populated.
        if (getSecretValueResult.getSecretString() != null) {
            secret = getSecretValueResult.getSecretString();
        }
        else {
            decodedBinarySecret = new String(Base64.getDecoder().decode(getSecretValueResult.getSecretBinary()).array());
        }

        return new JSONObject(secret);
    }
    


    public String getDbUrl() {
        return dbUrl;
    }

    public String getDbUser() {
        return dbUser;
    }

    public String getDbPass() {
        return dbPass;
    }

    public String getDbUrlRO() {
        return dbUrlRO;
    }

    public String getDbUserRO() {
        return dbUserRO;
    }

    public String getDbPassRO() {
        return dbPassRO;
    }

    public String getRabbitmqUri() {
        return rabbitmqUri;
    }

    public String getApnsProductId() {
        return apnsProductId;
    }

    public String getApnsCertPass() {
        return apnsCertPass;
    }

    public String getApnsCert() {
        return apnsCert;
    }

    public String getFcmDbUrl() {
        return fcmDbUrl;
    }

    public String getFcmConfig() {
        return fcmConfig;
    }

    public String getIosQueueName() {
        return iosQueueName;
    }

    public String getAndroidQueueName() {
        return androidQueueName;
    }

    public String getBouncedQueueName() {
        return bouncedQueueName;
    }

    public String getExchangeName() {
        return exchangeName;
    }

    public long getBatchSize() {
        return batchSize;
    }

    public int getPushThreads() {
        return pushThreads;
    }

    public int getInactivateThreads() {
        return inactivateThreads;
    }

    public JSONObject getKafkaTopics() {
        return kafkaTopics;
    }

    public int getFeedbackServiceInterval() {
        return feedbackServiceInterval;
    }

    public boolean isUseAPNSProdServer() {
        return useAPNSProdServer;
    }

    public long getTrackerAndroidDelay() {
        return trackerAndroidDelay;
    }

    public void setTrackerAndroidDelay(long trackerAndroidDelay) {
        this.trackerAndroidDelay = trackerAndroidDelay;
    }

    public long getTrackerIosDelay() {
        return trackerIosDelay;
    }

    public long getTrackerAndroidInitialDelay() {
        return trackerAndroidInitialDelay;
    }

    public long getTrackerIosInitialDelay() {
        return trackerIosInitialDelay;
    }

    public long getFeedbackServiceNumRetries() {
        return feedbackServiceNumRetries;
    }

    public void setFeedbackServiceNumRetries(long feedbackServiceNumRetries) {
        this.feedbackServiceNumRetries = feedbackServiceNumRetries;
    }

    public int getEventApiBatchSize() {
        return eventApiBatchSize;
    }

    public void setEventApiBatchSize(int eventApiBatchSize) {
        this.eventApiBatchSize = eventApiBatchSize;
    }

    public int getEventApiRateLimit() {
        return eventApiRateLimit;
    }

    public void setEventApiRateLimit(int eventApiRateLimit) {
        this.eventApiRateLimit = eventApiRateLimit;
    }

    public int getEventApiRetries() {
        return eventApiRetries;
    }

    public void setEventApiRetries(int eventApiRetries) {
        this.eventApiRetries = eventApiRetries;
    }

    public long getTrackerDelay() {
        return trackerDelay;
    }

    public void setTrackerDelay(long trackerDelay) {
        this.trackerDelay = trackerDelay;
    }

    public long getTrackerInitialDelay() {
        return trackerInitialDelay;
    }

    public void setTrackerInitialDelay(long trackerInitialDelay) {
        this.trackerInitialDelay = trackerInitialDelay;
    }

    public String getPublisherQueueName() {
        return publisherQueueName;
    }

    public String getListenerQueueName() {
        return listenerQueueName;
    }

    public void setListenerQueueName(String listenerQueueName) {
        this.listenerQueueName = listenerQueueName;
    }

    public String getAthenaRegion() {
        return athenaRegion;
    }

    public String getAthenaOutputBucket() {
        return athenaOutputBucket;
    }

    public String getAthenaCatalog() {
        return athenaCatalog;
    }

    public String getAthenaDB() {
        return athenaDB;
    }

    public String getDbExporterIdentifier() {
        return dbExporterIdentifier;
    }

    public String getDbExporterDbInstanceIdentifier() {
        return dbExporterDbInstanceIdentifier;
    }

    public String getDbExporterSnapshotPrefix() {
        return dbExporterSnapshotPrefix;
    }
}
