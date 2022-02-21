package com.ibm.weather.airlytics.jobs.eventspatch.config;

import com.ibm.weather.airlytics.common.rest.RetryableRestTemplateConfiguration;
import io.prometheus.client.CollectorRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@ComponentScan(basePackages = {"com.ibm.weather.airlytics.jobs.eventspatch","com.ibm.weather.airlytics.common.athena"})
@EnableAsync
@EnableScheduling
public class EventsPatchJobConfiguration {

    @Bean
    public CollectorRegistry collectorRegistry() {
        return CollectorRegistry.defaultRegistry;
    }

}
