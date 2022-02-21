package com.ibm.airlytics.airlock;

import com.ibm.airlock.common.AirlockCallback;
import com.ibm.airlock.common.AirlockInvalidFileException;
import com.ibm.airlock.common.AirlockNotInitializedException;
import com.ibm.airlock.common.AirlockProductManager;
import com.ibm.airlock.common.net.AirlockDAO;
import com.ibm.airlock.sdk.AirlockMultiProductsManager;
import com.ibm.airlock.sdk.cache.InstanceContext;
import com.ibm.airlock.sdk.cache.pref.FilePreferencesFactory;
import com.ibm.airlock.sdk.util.ProductLocaleProvider;
import com.ibm.airlytics.utilities.Environment;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class AirlockManager {
    private static final AirlyticsLogger AIRLYTICS_LOGGER = AirlyticsLogger.getLogger(AirlockManager.class.getName());
    private static final Logger LOGGER = Logger.getLogger(AirlyticsLogger.class.getName());
    private static AirlockManager instance;

    private static final int DEFAULT_PULL_TIMEOUT = 30;
    private static final String USER_GROUPS_ENV_VARIABLE_DELIMITER = ";";
    private static final String USER_GROUPS_ENV_VARIABLE_NAME = "AIRLOCK_USER_GROUPS";
    private static final String AIRLYTICS_ENVIRONMENT_ENV_VARIABLE = "AIRLYTICS_ENVIRONMENT";
    private static final String AIRLYTICS_PRODUCT_ENV_VARIABLE = "AIRLYTICS_PRODUCT";
    private static final String AIRLYTICS_DEPLOYMENT_ENV_VARIABLE = "AIRLYTICS_DEPLOYMENT";
    private static final String AIRLYTICS_CONSUMER_NAME_ENV_VARIABLE = "AIRLYTICS_CONSUMER_NAME";
    public static final String AIRLYTICS_RAWDATA_SOURCE_PARAM = "AIRLYTICS_RAWDATA_SOURCE";

    private String consumerType = "";

    private AirlockProductManager airlockManager;

    public static AirlockManager getInstance() {
        if (instance == null) {
            instance = new AirlockManager(null);
        }
        return instance;
    }

    public static AirlockManager getInstance(String consumerType) {
        if (instance == null) {
            instance = new AirlockManager(consumerType);
        }
        return instance;
    }

    public static String getDeployment() {
        return Environment.getAlphanumericEnv(AIRLYTICS_DEPLOYMENT_ENV_VARIABLE, false);
    }

    public static String getProduct() {
        return Environment.getAlphanumericEnv(AIRLYTICS_PRODUCT_ENV_VARIABLE, false);
    }

    public static String getEnvVar() {
        return Environment.getAlphanumericEnv(AIRLYTICS_ENVIRONMENT_ENV_VARIABLE, false).toLowerCase();
    }

    public static AirlockProductManager getAirlock() {
        return getInstance().airlockManager;
    }

    public static void close() {
        AirlockMultiProductsManager.getInstance().close();
    }

    public boolean refreshConfiguration() {
        try {
            pullAirlock(true);
            airlockManager.calculateFeatures(null, createCalculationContext());
            airlockManager.syncFeatures();
            return true;
        } catch (AirlockNotInitializedException e) {
            AIRLYTICS_LOGGER.error(e.getMessage());
            return false;
        }
    }

    public AirlockManager(String consumerType) {
        this.consumerType = consumerType;
        final AirlockMultiProductsManager airlockProductsManager = AirlockMultiProductsManager.getInstance();

        try {
            String airlockDefaultsFile = readAirlockDefaultFile();
            InstanceContext airlockProductInstance = new InstanceContext("KafkaConsumer",
                    FilePreferencesFactory.getAirlockCacheDirectory(), airlockDefaultsFile, "1.0");
            airlockManager = airlockProductsManager.createProduct(airlockProductInstance);
            airlockManager.initSDK(airlockProductInstance, airlockDefaultsFile, "1.0", "");
            // Use "responsive-mode" (e.g. no CDN, read directly from s3).
            airlockManager.setDataProviderType(AirlockDAO.DataProviderType.DIRECT_MODE);
            LOGGER.info("Airlock Product Instance [" +
                    airlockProductInstance.getProductName() + "] for app version [" + airlockProductInstance.getAppVersion() + "] was init successfully");
            // this.setUserGroups(System.getenv(USER_GROUPS_ENV_VARIABLE_NAME));
            this.setUserGroups(Environment.getAlphanumericEnv(USER_GROUPS_ENV_VARIABLE_NAME, true));
            LOGGER.info("Airlock user group [" + Environment.getAlphanumericEnv(USER_GROUPS_ENV_VARIABLE_NAME, true) + "] has been set");
            refreshConfiguration();
        } catch (IOException | AirlockInvalidFileException e) {
            LOGGER.error(e.getMessage());
        }
    }

    private JSONObject createCalculationContext() {
        String airlyticsEnvironment = Environment.getAlphanumericEnv(AIRLYTICS_ENVIRONMENT_ENV_VARIABLE, false);
        String airlyticsProduct = Environment.getAlphanumericEnv(AIRLYTICS_PRODUCT_ENV_VARIABLE, false);
        String airlyticsDeployment = Environment.getAlphanumericEnv(AIRLYTICS_DEPLOYMENT_ENV_VARIABLE, false);
        String airlyticsRawDataSource = Environment.getAlphanumericEnv(AIRLYTICS_RAWDATA_SOURCE_PARAM, true);
        String consumerName = Environment.getAlphanumericEnv(AIRLYTICS_CONSUMER_NAME_ENV_VARIABLE, true);

        JSONObject context = new JSONObject();
        context.put("environment", airlyticsEnvironment);
        context.put("product", airlyticsProduct);
        context.put("deployment", airlyticsDeployment);
        context.put("rawDataSource", airlyticsRawDataSource);
        context.put("consumerType", getConsumerType());

        // the extra env var is required to use in the airlock rule.
        // the param is used to assign topic in `Analytics Consumer` configuration
        context.put("consumerName", consumerName);

        LOGGER.info("context = " + context.toString());
        return context;
    }

    public CountDownLatch pullAirlock(boolean wait) {
        CountDownLatch retVal = null;
        try {
            if (airlockManager != null) {

                CountDownLatch pullLatch = new CountDownLatch(1);

                // TODO: optimize to act only if something changed
                airlockManager.pullFeatures(new AirlockCallback() {
                    @Override
                    public void onFailure(Exception e) {
                        AIRLYTICS_LOGGER.error(e.getMessage());
                        pullLatch.countDown();
                    }

                    @Override
                    public void onSuccess(String s) {
                        pullLatch.countDown();
                    }
                });

                // This operation will be blocked only if the "wait" parameter is true
                if (wait) {
                    if (!pullLatch.await(DEFAULT_PULL_TIMEOUT, TimeUnit.SECONDS)) {
                        AIRLYTICS_LOGGER.error("Time out happened on pull remote configuration");
                    }
                } else {
                    retVal = pullLatch;
                }
            }
        } catch (AirlockNotInitializedException | InterruptedException e) {
            AIRLYTICS_LOGGER.error(e.getMessage());
        }

        return retVal;
    }

    public String readAirlockDefaultFile() throws IOException {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("AirlockDefaults.json")) {
            return IOUtils.toString(inputStream, "UTF-8");
        }
    }

    public void setUserGroups(String userGroupsString) {

        if (userGroupsString == null) {
            return;
        }

        String[] groups = userGroupsString.split(USER_GROUPS_ENV_VARIABLE_DELIMITER);

        this.airlockManager.setDeviceUserGroups(Arrays.asList(groups));
    }

    public void setLocale(String localeStr) {
        airlockManager.setLocaleProvider(new ProductLocaleProvider(localeStr));
    }

    public String getConsumerType() {
        return consumerType;
    }

    public void setConsumerType(String consumerType) {
        this.consumerType = consumerType;
    }
}
