package com.ibm.weather.airlytics.jobs.dsr.monitoring;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/healthcheck")
public class HealthCheckController {

    @GetMapping(path = "/is-alive")
    @ResponseStatus(value = HttpStatus.OK)
    public void isAlive() {
        // do nothing
    }

    @GetMapping(path = "/is-ready")
    @ResponseStatus(value = HttpStatus.OK)
    public void isReady() {
        // do nothing
    }

}
