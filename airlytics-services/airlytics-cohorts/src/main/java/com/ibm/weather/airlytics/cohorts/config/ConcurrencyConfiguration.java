package com.ibm.weather.airlytics.cohorts.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
@EnableRetry
@EnableAsync
@EnableScheduling
public class ConcurrencyConfiguration {

    @Bean(name = "calculationTaskAsyncExecutor")
    public Executor getAsyncExecutor() {
        // single thread, endless queue
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.initialize();
        return executor;
    }

    @Bean(name = "airlockUpdateAsyncExecutor")
    public Executor threadPoolTaskExecutor() {
        // new thread for each task
        return new SimpleAsyncTaskExecutor();
    }
}
