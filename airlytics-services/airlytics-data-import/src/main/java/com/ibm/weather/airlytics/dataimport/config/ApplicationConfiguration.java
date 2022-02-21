package com.ibm.weather.airlytics.dataimport.config;

import com.ibm.weather.airlytics.common.rest.RetryableRestTemplateConfiguration;
import io.prometheus.client.CollectorRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(RetryableRestTemplateConfiguration.class)
public class ApplicationConfiguration {

    @Bean
    public CollectorRegistry collectorRegistry() {
        return CollectorRegistry.defaultRegistry;
    }
}
