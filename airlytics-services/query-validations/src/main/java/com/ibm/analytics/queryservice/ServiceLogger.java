package com.ibm.analytics.queryservice;

import com.ibm.analytics.queryservice.airlock.AirlockManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceLogger {

    private enum LEVEL{
        ERROR("L2:"),
        FATAL("FATAL:error");
        String name;
        LEVEL(String name) {
            this.name = name;
        }
    }

    private static final String OWNER_ENV_VAR = "POD_OWNER";

    public static void setSlackLoggerEnabled(boolean slackLoggerEnabled) {
        ServiceLogger.slackLoggerEnabled = slackLoggerEnabled;
    }

    private static boolean slackLoggerEnabled = true;

    private final Logger logger;

    private ServiceLogger(String className, boolean isEnabled) {
        slackLoggerEnabled = isEnabled;
        logger = LoggerFactory.getLogger(className);
    }

    public static ServiceLogger getLogger(String className) {
        return new ServiceLogger(className, AirlockManager.getInstance().getFeature("logs.Airlytics Logger").isOn());
    }

    public static ServiceLogger getLogger(String className, boolean isEnabled) {
        return new ServiceLogger(className, false);
    }

    private static String getLayeredMessage(ServiceLogger.LEVEL level, String message) {
        if (message==null) return null;
        if (slackLoggerEnabled){
            message = String.format("%s %s", level.name, message);
        }
        return message;
    }

    public void debug(String msg) {
        logger.debug(msg);
    }

    public void warn(String msg) {
        logger.warn(msg);
    }

    public void warn(String msg, Throwable t) { logger.warn(msg, t); }

    public void error(String msg) {
        String layeredMessage = ServiceLogger.getLayeredMessage(LEVEL.ERROR,msg);
        logger.error(layeredMessage);
    }

    public void error(String msg, Throwable t) {
        String layeredMessage = ServiceLogger.getLayeredMessage(LEVEL.ERROR,msg);
        logger.error(layeredMessage, t);
    }

    public void fatal(String msg) {
        String layeredMessage = ServiceLogger.getLayeredMessage(LEVEL.FATAL,msg);
        logger.error(layeredMessage);
    }

    public void info(String msg) {
        logger.info(msg);
    }

}
