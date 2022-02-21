package com.ibm.airlytics.consumer.integrations.mappings;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.RateLimiter;
import com.ibm.airlytics.consumer.cloning.config.ObfuscationConfig;
import com.ibm.airlytics.consumer.cloning.obfuscation.Obfuscator;
import com.ibm.airlytics.consumer.integrations.dto.AirlyticsEvent;
import com.ibm.airlytics.consumer.integrations.dto.ProductDefinition;
import com.ibm.airlytics.eventproxy.EventApiClient;
import com.ibm.airlytics.eventproxy.EventApiClientConfig;
import com.ibm.airlytics.eventproxy.EventProxyIntegrationConfig;
import com.ibm.airlytics.utilities.Hashing;
import com.ibm.airlytics.utilities.datamarker.ReadProgressMarker;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AbstractThirdPartyEventsFileProcessor {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(AbstractThirdPartyEventsFileProcessor.class.getName());

    protected List<ProductDefinition> products;

    protected ReadProgressMarker readProgressMarker;

    protected AbstractThirdPartyEventTransformer eventTransformer;

    protected Map<String, EventApiClient> apiClients;

    protected Map<String, EventApiClient> devApiClients;

    private Hashing hashing;

    private Map<String, RateLimiter> rateLimiters;

    private Obfuscator obfuscator;

    public AbstractThirdPartyEventsFileProcessor(
            EventApiClientConfig eventApiClientConfig,
            EventProxyIntegrationConfig eventProxyIntegrationConfig,
            List<ProductDefinition> products,
            ReadProgressMarker readProgressMarker,
            int partitionsNumber) {
        this(eventApiClientConfig, eventProxyIntegrationConfig, null, products, readProgressMarker, partitionsNumber);
    }

    public AbstractThirdPartyEventsFileProcessor(
            EventApiClientConfig eventApiClientConfig,
            EventProxyIntegrationConfig eventProxyIntegrationConfig,
            AbstractThirdPartyEventTransformer eventTransformer,
            List<ProductDefinition> products,
            ReadProgressMarker readProgressMarker,
            int partitionsNumber) {
        this.products = products;
        this.readProgressMarker = readProgressMarker;
        this.eventTransformer = eventTransformer;

        if(products != null && eventApiClientConfig != null && eventProxyIntegrationConfig != null && eventProxyIntegrationConfig.isEventApiEnabled()) {
            this.apiClients = createProxyClients(eventApiClientConfig,false);
            this.devApiClients = createProxyClients(eventApiClientConfig,true);
        } else if(eventProxyIntegrationConfig != null && !eventProxyIntegrationConfig.isEventApiEnabled()) {
            LOGGER.warn("Event Proxy API is disabled in Airlock!");
        } else {
            throw new IllegalArgumentException("Invalid Airlock configuration");
        }

        if(products != null && eventProxyIntegrationConfig != null) {

            if (eventProxyIntegrationConfig.getEventApiRateLimit() > 0) {
                this.rateLimiters = new HashMap<>();
                products.forEach(p ->
                         rateLimiters.put(
                                 p.getId(),
                                 RateLimiter.create(
                                         eventProxyIntegrationConfig.getEventApiRateLimit())));
            }
        }

        if(eventProxyIntegrationConfig != null && eventProxyIntegrationConfig.getObfuscation() != null) {
            this.obfuscator = new Obfuscator(eventProxyIntegrationConfig.getObfuscation());
        }
        this.hashing = new Hashing(partitionsNumber);
    }

    private Map<String, EventApiClient> createProxyClients(EventApiClientConfig eventApiClientConfig, boolean isDev) {
        Map<String, EventApiClient> toRet = new HashMap<>();

        for (ProductDefinition prodDef : this.products) {
            EventApiClient client =
                    new EventApiClient(
                            eventApiClientConfig,
                            isDev ? prodDef.getDevEventApiKey() : prodDef.getEventApiKey());
            LOGGER.info("Created" + (isDev?" dev ":" prod ") + "client for product " + prodDef.getId());
            toRet.put(prodDef.getId(), client);
        }
        return toRet;
    }

    protected EventApiClient getProxyClient(String productId, boolean isDev) {

        if(isDev) {
            return this.devApiClients == null ? null : this.devApiClients.get(productId);
        }
        return this.apiClients == null ? null : this.apiClients.get(productId);
    }

    protected void acquirePermit(String productId) {

        if (this.rateLimiters != null) {
            RateLimiter rl = this.rateLimiters.get(productId);

            if(rl != null) {
                rl.acquire();
            }
        }
    }

    protected int getUserPartition(String userId) {
        return hashing.hashMurmur2(userId);
    }

    protected void obfuscateIfNeeded(JsonNode event) {

        if(this.obfuscator != null && this.obfuscator.hasRules()) {
            obfuscator.obfuscate(event);
        }
    }
}
