package com.ibm.weather.airlytics.jobs.aggregate;

import com.ibm.weather.airlytics.jobs.aggregate.services.EventAggregatorConfigService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = {"com.ibm.weather.airlytics.jobs.aggregate","com.ibm.weather.airlytics.common.athena"})
public class TestConfiguration {

    @Bean
    public EventAggregatorConfigService ltvAggregatorConfigService() {
        return new TestEventAggregatorConfigService();
    }
}
