package com.ibm.airlytics.retentiontrackerpushhandler.airlock;

import com.ibm.airlytics.retentiontracker.airlock.AirlockManager;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component("Airlock")
public class Airlock {
    public Airlock() {
        AirlockManager.getInstance();
    }

    @Scheduled(fixedDelay = 600000)
    public void refreshAirlock() {
        if (AirlockManager.getAirlock().refreshConfiguration()) {
            //update what's needed
        }
    }
}
