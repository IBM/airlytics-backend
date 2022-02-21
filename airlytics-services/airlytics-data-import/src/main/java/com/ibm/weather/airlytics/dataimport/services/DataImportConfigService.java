package com.ibm.weather.airlytics.dataimport.services;

import com.ibm.weather.airlytics.dataimport.dto.DataImportAirlockConfig;
import com.ibm.weather.airlytics.dataimport.integrations.DataImportFeatureConfigManager;
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
public class DataImportConfigService {

    private static final Logger logger = LoggerFactory.getLogger(DataImportConfigService.class);

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private DataImportAirlockConfig currentConfig;

    private DataImportFeatureConfigManager airlock;

    @Autowired
    public DataImportConfigService(DataImportFeatureConfigManager airlock) {
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

    public Optional<DataImportAirlockConfig> getCurrentConfig() {
        rwLock.readLock().lock();
        try {
            return Optional.ofNullable(currentConfig);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    protected void updateCurrentConfig() {
        rwLock.writeLock().lock();

        try {
            currentConfig = airlock.readFromAirlockFeature();
        } catch (IOException e) {
            logger.error("Error reading feature config from airlock", e);
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
