package com.ibm.weather.airlytics.dataimport.monitoring;

import com.ibm.weather.airlytics.dataimport.db.UserFeaturesDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/healthcheck")
public class HealthCheckController {

    @Autowired
    private UserFeaturesDao db;

    @GetMapping(path = "/is-alive")
    @ResponseStatus(value = HttpStatus.OK)
    public void isAlive() {
        connectToDb();
    }

    @GetMapping(path = "/is-ready")
    @ResponseStatus(value = HttpStatus.OK)
    public void isReady() {
        connectToDb();
    }

    private void connectToDb() {

        if(db.validateConnection() != 1L) {
            throw new IllegalStateException("DB Connection test failed!");
        }
    }
}
