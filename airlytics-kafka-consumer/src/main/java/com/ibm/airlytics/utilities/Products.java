package com.ibm.airlytics.utilities;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.ibm.airlytics.consumer.dsr.DSRConsumerConfig;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class Products {
    private static final Logger LOGGER = Logger.getLogger(Products.class.getName());
    private static Products prods;

    static {
        try {
            prods = new Products();
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("cannot initiate Products configuration");
        }
    }

    public static List<Product> getProducts() {
        Map<String,Product> products = prods.config.getProducts();
        if (products==null) return new ArrayList<>();
        return new ArrayList<>(products.values());
    }

    public static Product getProductById(String id) {
        Map<String,Product> products = prods.config.getProducts();
        if (products==null) return null;
        return products.get(id);
    }

    public static List<Product> getProductsByPlatform(String platform) {
        Map<String,Product> products = prods.config.getProducts();
        List<Product> toRet = new ArrayList<>();
        if (products != null) {
            for (Product prod : products.values()) {
                if (prod.getPlatform().equals(platform)) {
                    toRet.add(prod);
                }
            }
        }
        return toRet;
    }

    public static void newConfigurationAvailable() {
        prods.newConfigAvailable();
    }

    private ProductsConfig config;
    private Products() throws IOException {
        config = new ProductsConfig();
        config.initWithAirlock();
    }

    private synchronized void newConfigAvailable() {
        ProductsConfig config = new ProductsConfig();
        try {
            config.initWithAirlock();
            this.config = config;
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.warn("error updating products config:"+e.getMessage());
        }
    }
}
