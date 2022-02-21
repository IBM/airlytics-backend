package com.ibm.airlytics.utilities;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlock.common.data.Feature;
import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.airlock.AirlockManager;
import org.json.JSONObject;

import javax.annotation.CheckForNull;
import java.io.IOException;
import java.util.Map;

public class ProductsConfig {
    private Map<String, Product> products;
    protected static final ObjectMapper objectMapper = new ObjectMapper();

    public Map<String, Product> getProducts() {
        return products;
    }

    public void setProducts(Map<String, Product> products) {
        this.products = products;
    }

    @CheckForNull
    protected String getAirlockConfiguration(String featureName) throws IOException {
        Feature feature = AirlockManager.getAirlock().getFeature(featureName);
        if (!feature.isOn())
            throw new IOException("Airlock feature " + featureName + " is not ON (" + feature.getSource()
                    + ", " + feature.getTraceInfo() + ")");

        JSONObject config = feature.getConfiguration();
        return config == null ? null : config.toString();
    }

    protected void readFromAirlockFeature(String featureName) throws IOException {
        String configuration = getAirlockConfiguration(featureName);
        if (configuration != null) {
            objectMapper.readerForUpdating(this).readValue(configuration);
        }
    }

    public void initWithAirlock() throws IOException {
        readFromAirlockFeature(AirlockConstants.airlytics.PRODUCTS_CONFIGURATION);
    }
}
