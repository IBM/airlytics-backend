package com.ibm.weather.airlytics.jobs.dsr;

import com.ibm.weather.airlytics.jobs.dsr.services.DsrConfigService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = {"com.ibm.weather.airlytics.jobs.dsr","com.ibm.weather.airlytics.common.athena"})
public class TestConfiguration {

    @Bean
    public DsrConfigService ltvAggregatorConfigService() {
        return new TestDsrConfigService();
    }
}
