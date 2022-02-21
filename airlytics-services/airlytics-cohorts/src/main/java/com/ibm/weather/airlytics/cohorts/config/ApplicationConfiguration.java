package com.ibm.weather.airlytics.cohorts.config;

import com.ibm.weather.airlytics.common.rest.RetryableRestTemplateConfiguration;
import io.prometheus.client.CollectorRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;

@Configuration
@Import(RetryableRestTemplateConfiguration.class)
public class ApplicationConfiguration {

    @Bean
    public CollectorRegistry collectorRegistry() {
        return CollectorRegistry.defaultRegistry;
    }

    @Bean(name = "datasourceRW")
    @ConfigurationProperties("spring.datasource")
    @Primary
    public DataSource dataSource(){
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "datasourceRO")
    @ConfigurationProperties("ro.datasource")
    public DataSource dataSourceRO(){
        return DataSourceBuilder.create().build();
    }

    @Bean
    @Autowired
    @Primary
    DataSourceTransactionManager tm(@Qualifier ("datasourceRW") DataSource datasource) {
        DataSourceTransactionManager txm  = new DataSourceTransactionManager(datasource);
        return txm;
    }
}
