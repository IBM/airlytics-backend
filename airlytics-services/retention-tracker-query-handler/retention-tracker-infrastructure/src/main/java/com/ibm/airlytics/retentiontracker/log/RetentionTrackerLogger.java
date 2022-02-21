package com.ibm.airlytics.retentiontracker.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetentionTrackerLogger {

    private Logger logger;
    private RetentionTrackerLogger(String className) {
        logger = LoggerFactory.getLogger(className);
    }
    public static RetentionTrackerLogger getLogger(String className) {
        return new RetentionTrackerLogger(className);
    }

    public void debug(String msg) {
        logger.debug(msg);
    }

    public void warn(String msg) {
        logger.warn(msg);
    }

    public void error(String msg) {
        logger.error(msg);
    }

    public void info(String msg) {
        logger.info(msg);
    }

}
