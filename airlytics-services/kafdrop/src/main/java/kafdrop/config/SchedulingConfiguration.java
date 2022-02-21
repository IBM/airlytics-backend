package kafdrop.config;

import io.prometheus.client.CollectorRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.annotation.PostConstruct;

@Configuration
@EnableAsync
@EnableScheduling
public class SchedulingConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(SchedulingConfiguration.class);

    @PostConstruct
    public void init() {
        LOG.info("SchedulingConfiguration instantiated");
    }

    @Bean
    public CollectorRegistry collectorRegistry() {
        return CollectorRegistry.defaultRegistry;
    }

}
