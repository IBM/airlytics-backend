package com.ibm.weather.airlytics.common.rest;

import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.client.OkHttp3ClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.*;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;

@Configuration
@ComponentScan
public class RetryableRestTemplateConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(RetryableRestTemplateConfiguration.class);

    private static final int HTTP_MAX_IDLE = 5;// connections
    private static final int HTTP_KEEP_ALIVE = 10;// seconds
    private static final int HTTP_CONNECTION_TIMEOUT = 120;// seconds

    public static final int RETRY_INIT_INTERVAL = 1000;// ms
    public static final int RETRY_MULTIPLIER = 2;
    public static final int RETRY_MAX_ATTEMPTS = 3;

    @Bean
    @Qualifier("okhttp3Template")
    public RestTemplate okhttp3Template() {

        try {
            RestTemplate restTemplate = new RestTemplate();

            // Create a trust manager that does not validate certificate chains
            final TrustManager[] trustAllCerts = new TrustManager[]{
                    new X509TrustManager() {
                        @Override
                        public void checkClientTrusted(java.security.cert.X509Certificate[] chain,
                                                       String authType) throws CertificateException {
                        }

                        @Override
                        public void checkServerTrusted(java.security.cert.X509Certificate[] chain,
                                                       String authType) throws CertificateException {
                        }

                        @Override
                        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                            return new X509Certificate[0];
                        }
                    }
            };

            // Install the all-trusting trust manager
            final SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
            // Create an ssl socket factory with our all-trusting manager
            final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();


            // 1. create the okhttp client builder
            OkHttpClient.Builder builder = new OkHttpClient.Builder()
                    .sslSocketFactory(sslSocketFactory, (X509TrustManager) trustAllCerts[0])
                    .hostnameVerifier(new HostnameVerifier() {
                        @Override
                        public boolean verify(String hostname, SSLSession session) {
                            return true;
                        }
                    });
            ConnectionPool okHttpConnectionPool =
                    new ConnectionPool(HTTP_MAX_IDLE, HTTP_KEEP_ALIVE, TimeUnit.SECONDS);
            builder.connectionPool(okHttpConnectionPool);
            builder.connectTimeout(HTTP_CONNECTION_TIMEOUT, TimeUnit.SECONDS);
            builder.retryOnConnectionFailure(false);

            // 2. embed the created okhttp client to a spring rest template
            restTemplate.setRequestFactory(new OkHttp3ClientHttpRequestFactory(builder.build()));

            return restTemplate;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Bean
    @Qualifier("httpClientTemplate")
    public RestTemplate httpClientRestTemplate() {
        CloseableHttpClient httpClient = HttpClients.custom()
                .setSSLHostnameVerifier(new NoopHostnameVerifier())
                .build();
        HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory();
        requestFactory.setHttpClient(httpClient);
        return new RestTemplate(requestFactory);
    }
}
