package com.ibm.weather.airlytics.jobs.eventspatch;

import com.ibm.weather.airlytics.jobs.eventspatch.services.EventsPatchConfigService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = {"com.ibm.weather.airlytics.jobs.eventspatch","com.ibm.weather.airlytics.common.athena"})
public class TestConfiguration {

    @Bean
    public EventsPatchConfigService ltvAggregatorConfigService() {
        return new TestEventsPatchConfigService();
    }
}
