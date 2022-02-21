package com.ibm.weather.airlytics.jobs.dsr.services;

import com.ibm.weather.airlytics.common.airlock.AirlockException;
import com.ibm.weather.airlytics.jobs.dsr.dto.DsrJobAirlockConfig;
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
public class DsrConfigService {

    private static final Logger logger = LoggerFactory.getLogger(DsrConfigService.class);

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private DsrJobAirlockConfig currentConfig;

    private DsrJobFeatureConfigManager airlock;

    @Autowired
    public DsrConfigService(DsrJobFeatureConfigManager airlock) {
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

    public Optional<DsrJobAirlockConfig> getCurrentConfig() {
        rwLock.readLock().lock();
        try {
            return Optional.ofNullable(currentConfig);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public DsrJobAirlockConfig getAirlockConfig() throws AirlockException {
        Optional<DsrJobAirlockConfig> optConfig = getCurrentConfig();

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
