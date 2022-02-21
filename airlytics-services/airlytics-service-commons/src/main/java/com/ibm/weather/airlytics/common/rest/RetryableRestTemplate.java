package com.ibm.weather.airlytics.common.rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.net.URI;

@Service
@Import(RetryableRestTemplateConfiguration.class)
public class RetryableRestTemplate {

    private RestTemplate okHttpRestTemplate;

    @Autowired
    public RetryableRestTemplate(@Qualifier("okhttp3Template") RestTemplate okHttpRestTemplate) {
        this.okHttpRestTemplate = okHttpRestTemplate;
    }

    @Retryable(
            value = { HttpServerErrorException.class,  ResourceAccessException.class },
            maxAttempts = RetryableRestTemplateConfiguration.RETRY_MAX_ATTEMPTS,
            backoff = @Backoff(delay = RetryableRestTemplateConfiguration.RETRY_INIT_INTERVAL, multiplier = RetryableRestTemplateConfiguration.RETRY_MULTIPLIER))
    public <T> ResponseEntity<T> execute(
            URI uri,
            HttpMethod httpMethod,
            HttpEntity<?> httpEntity,
            Class<T> responseEntityType) throws RestClientException {

        return okHttpRestTemplate.exchange(
                uri,
                httpMethod,
                httpEntity,
                responseEntityType);
    }
}
