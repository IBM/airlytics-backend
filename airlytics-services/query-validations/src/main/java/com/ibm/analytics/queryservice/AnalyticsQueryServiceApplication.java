package com.ibm.analytics.queryservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class AnalyticsQueryServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AnalyticsQueryServiceApplication.class, args);
    }

}
