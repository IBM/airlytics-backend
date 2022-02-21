package com.ibm.airlytics.retentiontrackerqueryhandler.airlock;

import com.ibm.airlytics.retentiontracker.airlock.AirlockManager;
import org.springframework.stereotype.Component;

@Component("Airlock")
public class Airlock {
    public Airlock() {
        AirlockManager.getInstance();
    }

//    @Scheduled(fixedDelay = 600000)
    public boolean refreshAirlock() {
        return AirlockManager.getAirlock().refreshConfiguration();
    }
}
