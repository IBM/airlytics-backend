package com.ibm.weather.airlytics.common.airlock;

import com.google.gson.JsonObject;
import com.ibm.weather.airlytics.common.rest.RetryableRestTemplate;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.util.DefaultUriBuilderFactory;
import org.springframework.web.util.UriBuilderFactory;

import javax.annotation.PostConstruct;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Component
public abstract class BaseAirlockClient {

    private static final Logger logger = LoggerFactory.getLogger(BaseAirlockClient.class);

    private static final long TOKEN_EXPIRATION_TIME = 60*60*1000L;//60 minutes
    private static final long TOKEN_RENEWAL_TIME = 50*60*1000L;//50 minutes

    private static final String AUTHENTICATION_PATH = "/admin/authentication/startSessionFromKey";
    private static final String EXTEND_PATH = "/admin/authentication/extend";
    protected static final String VALIDATION_PATH = "/admin/authentication/validate";

    protected static final String AUTHENTICATION_HEADER_NAME = "sessionToken";

    protected RetryableRestTemplate restTemplate;

    protected UriBuilderFactory uriBuilderFactory;

    @Value("${airlock.api.key:}")
    protected String airlockKey;

    @Value("${airlock.api.password:}")
    protected String airlockPassword;

    protected String airlockBaseUrl;

    private String lastToken = null;
    private long lastTokenTime = 0L;

    @Autowired
    public BaseAirlockClient(RetryableRestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @PostConstruct
    public void init() throws AirlockException {
        initBaseUrl();
        this.uriBuilderFactory = new DefaultUriBuilderFactory(airlockBaseUrl);
    }

    protected abstract void initBaseUrl() throws AirlockException;

    public String startSessionGetToken(boolean enforce) throws AirlockException {
        String token = null;

        if(StringUtils.isNoneBlank(airlockKey, airlockPassword)) {
            // auth required
            token = getAuthToken(enforce).orElseThrow(() -> new AirlockException("Airlock authentication failed"));
        } else if(!StringUtils.isAllBlank(airlockKey, airlockPassword)) {
            throw new AirlockException("Airlock authentication is mis-configured");
        }
        return token;
    }

    public boolean validateToken(String token) {

        if(StringUtils.isAllBlank(airlockKey, airlockPassword)) {// do not auth, if Airlock doesn't
            return true;
        }

        if(token == null) {
            logger.error("sessionToken header is missing");
            return false;
        }

        if(StringUtils.isBlank(token)) {
            logger.error("Empty token from caller");
            return false;
        }
        HttpEntity request = createEmptyRequestEntityWithAuth(token, Collections.singletonList(MediaType.ALL));

        try {
            URI url = uriBuilderFactory.builder().path(VALIDATION_PATH).build();
            ResponseEntity<Void> response = restTemplate.execute(url, HttpMethod.GET, request, Void.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                return true;
            } else {
                logger.error("Token {} is invalid", token);
                return false;
            }
        } catch(Exception e) {
            logger.error("Validation failed for token " + token + ": error calling Airlock API", e);
            return false;
        }
    }

    protected HttpEntity createEmptyRequestEntityWithAuth(String token, List<MediaType> accept) {
        HttpHeaders headers = new HttpHeaders();

        if(StringUtils.isNotBlank(token)) {
            headers.set(AUTHENTICATION_HEADER_NAME, token);
        }

        if(accept != null) {
            headers.setAccept(accept);
        }
        return new HttpEntity(headers);
    }

    protected HttpEntity createRequestEntityWithAuth(String body, String token, List<MediaType> accept) {
        HttpHeaders headers = new HttpHeaders();

        if(StringUtils.isNotBlank(token)) {
            headers.set(AUTHENTICATION_HEADER_NAME, token);
        }

        if(accept != null) {
            headers.setAccept(accept);
        }
        return new HttpEntity(body, headers);
    }

    protected Optional<String> getAuthToken(boolean enforce) throws AirlockException {

        if(!enforce && lastToken != null && (System.currentTimeMillis() - lastTokenTime) < TOKEN_RENEWAL_TIME) {
            return Optional.of(lastToken);
        }

        try {
            ResponseEntity<String> authResponse = null;

            if(!enforce && lastToken != null && (System.currentTimeMillis() - lastTokenTime) < TOKEN_EXPIRATION_TIME) {
                authResponse = getExtendedToken();
            } else {
                authResponse = getTokenFromApiKey();
            }

            if (authResponse.getStatusCode().is2xxSuccessful()) {
                lastToken = authResponse.getBody();
                lastTokenTime = System.currentTimeMillis();
                return Optional.ofNullable(lastToken);
            } else {
                logger.error("Authentication failed: response {} from Airlock API", authResponse.getStatusCodeValue());
            }
        } catch(Exception e) {
            logger.error("Authentication failed: error calling Airlock API", e);
            throw new AirlockException("Authentication failed: error calling Airlock API", e);
        }
        return Optional.empty();
    }

    private ResponseEntity<String> getExtendedToken() {

        try {
            HttpEntity request = createEmptyRequestEntityWithAuth(lastToken, null);
            URI url = uriBuilderFactory.builder().path(EXTEND_PATH).build();
            ResponseEntity<String> authResponse = restTemplate.execute(url, HttpMethod.GET, request, String.class);

            if (authResponse.getStatusCode().is4xxClientError()) {
                return getTokenFromApiKey();
            }
            return authResponse;
        } catch(HttpClientErrorException e) {

            if(e.getStatusCode().is4xxClientError()) {
                return getTokenFromApiKey();
            }
            throw e;
        } catch(Exception e) {
            throw e;
        }
    }

    private ResponseEntity<String> getTokenFromApiKey() {
        JsonObject content = new JsonObject();
        content.addProperty("key", airlockKey);
        content.addProperty("keyPassword", airlockPassword);
        HttpEntity<String> request = new HttpEntity<>(content.toString());

        URI url = uriBuilderFactory.builder().path(AUTHENTICATION_PATH).build();
        return restTemplate.execute(url, HttpMethod.POST, request, String.class);
    }
}
