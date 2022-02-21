package com.ibm.weather.airlytics.jobs.eventspatch.monitoring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.atomic.AtomicInteger;

@RestController
@RequestMapping("/healthcheck")
public class HealthCheckController {

    private static final Logger logger = LoggerFactory.getLogger(HealthCheckController.class);

    private static final AtomicInteger count = new AtomicInteger(0);

    @GetMapping(path = "/is-alive")
    @ResponseStatus(value = HttpStatus.OK)
    public void isAlive() {
        if(count.getAndIncrement() % 10 == 0) {
            logger.info("I'm alive!");
        }
    }

    @GetMapping(path = "/is-ready")
    @ResponseStatus(value = HttpStatus.OK)
    public void isReady() {
        logger.info("I'm ready!");
    }

}
