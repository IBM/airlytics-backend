package com.ibm.weather.airlytics.common.airlock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlock.common.AirlockCallback;
import com.ibm.airlock.common.AirlockInvalidFileException;
import com.ibm.airlock.common.AirlockNotInitializedException;
import com.ibm.airlock.common.AirlockProductManager;
import com.ibm.airlock.common.data.Feature;
import com.ibm.airlock.common.net.AirlockDAO;
import com.ibm.airlock.sdk.AirlockMultiProductsManager;
import com.ibm.airlock.sdk.cache.InstanceContext;
import com.ibm.airlock.sdk.cache.pref.FilePreferencesFactory;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Component
public class BaseAirlockFeatureConfigManager {

    private static final Logger logger = LoggerFactory.getLogger(BaseAirlockFeatureConfigManager.class);

    private static final int DEFAULT_PULL_TIMEOUT = 30;

    @Value("#{'${AIRLOCK_USER_GROUPS:}'.split(',')}")
    private List<String> userGroups;

    @Value("${airlock.airlytics.env}")
    private String airlyticsEnvironment;

    @Value("${airlock.airlytics.deployment}")
    private String airlyticsDeployment;

    @Value("${airlock.airlytics.product}")
    private String airlyticsProduct;

    @Value("${airlock.airlytics.feature}")
    private String airlyticsFeature;

    private AirlockProductManager airlockManager;

    public BaseAirlockFeatureConfigManager() {}

    @PostConstruct
    public void init() {
        final AirlockMultiProductsManager airlockProductsManager = AirlockMultiProductsManager.getInstance();

        try {
            String airlockDefaultsFile = readAirlockDefaultFile();
            InstanceContext airlockProductInstance = new InstanceContext("KafkaConsumer",
                    FilePreferencesFactory.getAirlockCacheDirectory(), airlockDefaultsFile, "1.0");
            airlockManager = airlockProductsManager.createProduct(airlockProductInstance);
            airlockManager.initSDK(airlockProductInstance, airlockDefaultsFile, "1.0", "");
            airlockManager.setDataProviderType(AirlockDAO.DataProviderType.DIRECT_MODE);
            logger.info("Airlock Product Instance [" +
                    airlockProductInstance.getProductName() + "] for app version [" + airlockProductInstance.getAppVersion() + "] was init successfully");

            if(CollectionUtils.isNotEmpty(userGroups) && !userGroups.contains("")) {
                this.airlockManager.setDeviceUserGroups(userGroups);
                logger.info("Airlock user groups {} have been set", userGroups);
            }
            refreshConfiguration();
        } catch (IOException | AirlockInvalidFileException e) {
            logger.error(e.getMessage());
        }
    }

    public boolean refreshConfiguration() {
        try {
            pullAirlock(true);
            airlockManager.calculateFeatures(null, createCalculationContext());
            airlockManager.syncFeatures();
            return true;
        } catch (AirlockNotInitializedException e) {
            logger.error(e.getMessage());
            return false;
        }
    }

    protected String getAirlockConfiguration() throws IOException {
        Feature feature = airlockManager.getFeature(airlyticsFeature);
        if (!feature.isOn())
            throw new IOException("Airlock feature " + airlyticsFeature + " is not ON ("+ feature.getSource()
                    + ", " + feature.getTraceInfo() + ")");

        JSONObject config = feature.getConfiguration();
        return config.toString();
    }

    protected JSONObject createCalculationContext() {

        JSONObject context = new JSONObject();
        context.put("environment", airlyticsEnvironment);
        context.put("deployment", airlyticsDeployment);
        context.put("product", airlyticsProduct);

        logger.debug("context = " + context.toString());
        return context;
    }

    private CountDownLatch pullAirlock(boolean wait) {
        CountDownLatch retVal = null;
        try {
            if (airlockManager != null) {

                CountDownLatch pullLatch = new CountDownLatch(1);

                airlockManager.pullFeatures(new AirlockCallback() {
                    @Override
                    public void onFailure(Exception e) {
                        logger.error(e.getMessage());
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
                        logger.error("Time out happened on pull remote configuration");
                    }
                } else {
                    retVal = pullLatch;
                }
            }
        } catch (AirlockNotInitializedException | InterruptedException e) {
            logger.error(e.getMessage());
        }

        return retVal;
    }

    private String readAirlockDefaultFile() throws IOException {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("AirlockDefaults.json")) {
            return IOUtils.toString(inputStream, "UTF-8");
        }
    }
}
