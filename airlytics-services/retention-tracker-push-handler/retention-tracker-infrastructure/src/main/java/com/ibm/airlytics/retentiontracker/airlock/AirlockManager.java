package com.ibm.airlytics.retentiontracker.airlock;

import com.ibm.airlock.common.AirlockCallback;
import com.ibm.airlock.common.AirlockInvalidFileException;
import com.ibm.airlock.common.AirlockNotInitializedException;
import com.ibm.airlock.common.AirlockProductManager;
import com.ibm.airlock.common.net.AirlockDAO;
import com.ibm.airlock.sdk.AirlockMultiProductsManager;
import com.ibm.airlock.sdk.cache.InstanceContext;
import com.ibm.airlock.sdk.cache.pref.FilePreferencesFactory;
import com.ibm.airlock.sdk.util.ProductLocaleProvider;
import com.ibm.airlytics.retentiontracker.utils.Environment;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class AirlockManager {
    private RetentionTrackerLogger LOGGER = RetentionTrackerLogger.getLogger(AirlockManager.class.getName());
    private static AirlockManager instance;

    private final int DEFAULT_PULL_TIMEOUT = 30;
    private final String USER_GROUPS_ENV_VARIABLE_DELIMITER = ";";
    private final String USER_GROUPS_ENV_VARIABLE_NAME = "AIRLOCK_USER_GROUPS";
    private final String AIRLYTICS_ENVIRONMENT = "AIRLYTICS_ENVIRONMENT";
    private final String AIRLYTICS_DEPLOYMENT = "AIRLYTICS_DEPLOYMENT";
    private final static String TYPE_VARIABLE = "USERDB_PROCESS_TYPE";
    private final String AIRLYTICS_PRODUCT = "AIRLYTICS_PRODUCT";

    private AirlockProductManager airlockManager;

    public static AirlockManager getAirlock() {
        if (instance == null) {
            instance = new AirlockManager();
        }
        return instance;
    }

    public static AirlockProductManager getInstance() {
        return getAirlock().airlockManager;
    }

    public static void close() {
        AirlockMultiProductsManager.getInstance().close();
    }

    public AirlockManager() {
        final AirlockMultiProductsManager airlockProductsManager = AirlockMultiProductsManager.getInstance();

        try {
            String airlockDefaultsFile = readAirlockDefaultFile().trim();
            JSONObject obj = new JSONObject(airlockDefaultsFile);
            InstanceContext airlockProductInstance = new InstanceContext("RetentionTracker",
                    FilePreferencesFactory.getAirlockCacheDirectory(), airlockDefaultsFile, "1.0");
            airlockManager = airlockProductsManager.createProduct(airlockProductInstance);
            airlockManager.initSDK(airlockProductInstance, airlockDefaultsFile, "1.0", "");
            airlockManager.setDataProviderType(AirlockDAO.DataProviderType.DIRECT_MODE);
            LOGGER.info("Airlock Product Instance [" +
                    airlockProductInstance.getProductName() + "] for app version [" + airlockProductInstance.getAppVersion() + "] was init successfully");
            this.setUserGroups(System.getenv(USER_GROUPS_ENV_VARIABLE_NAME));
            LOGGER.info("Airlock user group [" + System.getenv(USER_GROUPS_ENV_VARIABLE_NAME) + "] has been set");
            pullAirlock(true);
            airlockManager.calculateFeatures(null, createCalculationContext());
            airlockManager.syncFeatures();
        } catch (IOException | AirlockInvalidFileException | AirlockNotInitializedException e) {
            LOGGER.error(e.getMessage());
        }
    }

    public String getProduct() {
        String prod = Environment.getAlphanumericEnv(AIRLYTICS_PRODUCT, true);
        return prod != null ? prod : "";
    }

    public String getPlatform() {
        String type = Environment.getEnv(TYPE_VARIABLE, true);
        if (type == null) {
            return "unknown";
        } else if (type.toLowerCase().contains("ios")) {
            return "ios";
        }else if (type.toLowerCase().contains("android")) {
            return "android";
        } else {
            return "unknown";
        }
    }
    private JSONObject createCalculationContext() {
        String airlyticsEnvironment = System.getenv(AIRLYTICS_DEPLOYMENT);
        String airlyticsProduct = System.getenv(AIRLYTICS_PRODUCT);
        String airlyticsEnv = System.getenv(AIRLYTICS_ENVIRONMENT);
        String processType = System.getenv(TYPE_VARIABLE);
        JSONObject context = new JSONObject();
        context.put("environment", airlyticsEnvironment);
        context.put("env", airlyticsEnv);
        context.put("product", airlyticsProduct);
        context.put("processType", processType);
        LOGGER.info("Airlock context:"+context.toString());
        return context;
    }

    public CountDownLatch pullAirlock(boolean wait) {
        CountDownLatch retVal = null;
        try {
            if (airlockManager != null) {

                CountDownLatch pullLatch = new CountDownLatch(1);

                airlockManager.pullFeatures(new AirlockCallback() {
                    @Override
                    public void onFailure(Exception e) {
                        LOGGER.error(e.getMessage());
                        pullLatch.countDown();
                    }

                    @Override
                    public void onSuccess(String s) {
                        pullLatch.countDown();
                    }
                });

                // This operation will be blocked only if the "wait" parameter is true
                if (wait) {
                    pullLatch.await(DEFAULT_PULL_TIMEOUT, TimeUnit.SECONDS);
                } else {
                    retVal = pullLatch;
                }
            }
        } catch (AirlockNotInitializedException | InterruptedException e) {
            LOGGER.error(e.getMessage());
        }

        return retVal;
    }

    public boolean refreshConfiguration() {
        try {
            pullAirlock(true);
            airlockManager.calculateFeatures(null, createCalculationContext());
            airlockManager.syncFeatures();
            return true;
        } catch (AirlockNotInitializedException e) {
            LOGGER.error(e.getMessage());
            return false;
        }
    }

    public String readAirlockDefaultFile() throws IOException {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("AirlockDefaults.json")) {
            return removeUTF8BOM(IOUtils.toString(inputStream, "UTF-8"));
        }
    }

    public static final String UTF8_BOM = "\uFEFF";

    private String removeUTF8BOM(String s) {
        if (s.startsWith(UTF8_BOM)) {
            s = s.substring(1);
        }
        return s;
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

}
