package com.ibm.weather.airlytics.jobs.eventspatch.services;

import com.ibm.weather.airlytics.common.airlock.AirlockException;
import com.ibm.weather.airlytics.jobs.eventspatch.dto.EventsPatchAirlockConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Service
public class EventsPatchConfigService {

    private static final Logger logger = LoggerFactory.getLogger(EventsPatchConfigService.class);

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private EventsPatchAirlockConfig currentConfig;

    private EventsPatchFeatureConfigManager airlock;

    @Autowired
    public EventsPatchConfigService(EventsPatchFeatureConfigManager airlock) {
        this.airlock = airlock;
    }

    @PostConstruct
    public void init() {
        updateCurrentConfig();
    }

    @Scheduled(initialDelayString = "${airlock.feature.refresh.initDelay}", fixedDelayString = "${airlock.feature.refresh.delay}")
    public void refreshConfiguration() {
        if(airlock.refreshConfiguration()) {
            updateCurrentConfig();
        }
    }

    public Optional<EventsPatchAirlockConfig> getCurrentConfig() {
        rwLock.readLock().lock();
        try {
            return Optional.ofNullable(currentConfig);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public EventsPatchAirlockConfig getAirlockConfig() throws AirlockException {
        Optional<EventsPatchAirlockConfig> optConfig = getCurrentConfig();

        if(!optConfig.isPresent()) {
            throw new AirlockException("Error reading Airlock feature config");
        }
        return optConfig.get();
    }

    private void updateCurrentConfig() {
        rwLock.writeLock().lock();

        try {
            currentConfig = airlock.readFromAirlockFeature();
            logger.info("Airlock feature config appplied: {}", currentConfig);
        } catch (IOException e) {
            logger.error("Error reading feature config from airlock", e);
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
