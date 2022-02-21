package com.ibm.airlytics.utilities.logs;

import com.ibm.airlock.common.data.Feature;
import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.utilities.Environment;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.io.IOException;

public class AirlyticsLogger {
    private static final Logger LOGGER = Logger.getLogger(AirlyticsLogger.class.getName());
    private static final String OWNER_ENV_VAR = "POD_OWNER";
    private final Logger logger;
    private static boolean local = false;//set to true, when testing locally, for better performance and in uni-tests

    /**
     * CAUTION: setting value to `true` effectively *disables* all the functionality of AirlyticsLogger.
     * Set to `true` *only* in unit-tests.
     *
     * @param local
     */
    public static void setLocal(boolean local) {
        LOGGER.warn("AirlyticsLogger was set to work in local/test mode");
        AirlyticsLogger.local = local;
    }

    public String getClassName() {
        return className;
    }

    private final String className;

    private AirlyticsLogger(String className) {
        this.className = className;
        logger = Logger.getLogger(className);
    }

    private static String addProductAndOwner(String message, String layersPrefix) {
        if (message == null) return null;

        if(local) return message;

        try {
            Feature feature = AirlockManager.getAirlock().getFeature(AirlockConstants.logs.OWNERS_CONFIG);
            JSONObject config = feature.getConfiguration();
            if (config != null && feature.isOn()) {
                //wrapperTemplate
                String template = config.getString("wrapperTemplateWithLayer");
                //add owner if exists
                String owner = config.getString("owner");
                if (owner == null) owner = "unknown";

                //add product if exists
                String product = config.getString("product");
                if (product == null) product = "unknown";
                String processType = config.getString("processType");
                if (processType == null) processType = "unknown";
                String prefix = String.format(template, layersPrefix, product, owner, processType);
                message = prefix + message;
            }
        } catch (Exception e) {
            // ignore and log anyway
            System.err.println("Error in AirlyticsLogger, falling back to the basic logging");
        }
        return message;
    }

    private static String getLayeredMessage(String message, String className) {
        if (message == null) return null;

        if(local) return message;

        String pref = "";
        try {
            Feature feature = AirlockManager.getAirlock().getFeature(AirlockConstants.logs.AIRLYTICS_LOGGER);
            JSONObject config = feature.getConfiguration();
            //add configured prefixes
            for (Feature sub : feature.getChildren()) {
                if (sub.isOn()) {
                    AirlyticsLogLayerConfig layerConfig = new AirlyticsLogLayerConfig();
                    try {
                        layerConfig.initWithAirlock(sub.getName());
                    } catch (IOException e) {
                        LOGGER.error("error reading config of feature:" + sub.getName() + ":" + e.getMessage());
                        e.printStackTrace();
                        return message;
                    }
                    if (layerConfig.hasMatch(className, message) && config != null) {
                        pref = config.getString(layerConfig.getLogLevel()) + pref;
                    }
                }
            }

            //add owner name if exists
            String ownerName = Environment.getEnv(OWNER_ENV_VAR, true);
            if (ownerName != null && !ownerName.isEmpty()) {
                pref = String.format("%s:%s", ownerName, pref);
            }
        } catch (Exception e) {
            // ignore and log anyway
            System.err.println("Error in AirlyticsLogger, falling back to the basic logging");
        }
        return addProductAndOwner(message, pref);
    }

    public static AirlyticsLogger getLogger(String className) {
        return new AirlyticsLogger(className);
    }

    public void debug(String msg) {
        logger.debug(addProductAndOwner(msg,""));
    }

    public void warn(String msg) {
        logger.warn(addProductAndOwner(msg,""));
    }

    public void warn(String msg, Throwable t) {
        logger.warn(addProductAndOwner(msg,""), t);
    }

    public void error(String msg) {
        String layeredMessage = AirlyticsLogger.getLayeredMessage(msg, getClassName());
        logger.error(layeredMessage);
    }

    public void error(String msg, Throwable t) {
        String layeredMessage = AirlyticsLogger.getLayeredMessage(msg, getClassName());
        logger.error(layeredMessage, t);
    }

    public void info(String msg) {
        logger.info(addProductAndOwner(msg,""));
    }

}
