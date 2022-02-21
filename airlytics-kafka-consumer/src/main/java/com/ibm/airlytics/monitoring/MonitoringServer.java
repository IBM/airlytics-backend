package com.ibm.airlytics.monitoring;

import com.ibm.airlytics.consumer.AirlyticsConsumer;
import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.DefaultExports;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.util.Headers;

import javax.servlet.ServletException;

import static io.undertow.util.StatusCodes.*;

public class MonitoringServer {
    private final AirlyticsConsumer consumer;
    private Undertow server;
    private static final String HEALTH_PATH = "/healthcheck";
    private static final String MONITORING_PATH = "/metrics";

    private class HealthHandler implements HttpHandler  {

        @Override
        public void handleRequest(HttpServerExchange exchange) throws Exception {
            if (consumer.isHealthy())
                exchange.setStatusCode(OK);
            else
                exchange.setStatusCode(SERVICE_UNAVAILABLE);

            exchange.getResponseHeaders().add(Headers.CONTENT_TYPE, "text/plain");
            exchange.getResponseSender().send(consumer.getHealthMessage());
        }
    }

    public MonitoringServer(AirlyticsConsumer consumer) {
        this.consumer = consumer;
    }

    public void start() throws ServletException {
        start(8080);
    }

    public void start(int port) throws ServletException {
        // Initialize default JVM metrics for Prometheus
        DefaultExports.initialize();

        DeploymentInfo monitoringServletInfo = Servlets.deployment()
                .setClassLoader(MonitoringServer.class.getClassLoader())
                .setContextPath("/")
                .setDeploymentName("monitoring")
                .addServlet(Servlets.servlet(AirlyticsConsumerMetricsServlet.class).addMapping("/*"));

        DeploymentManager monitoringDeployment = Servlets.defaultContainer().addDeployment(monitoringServletInfo);
        monitoringDeployment.deploy();
        HttpHandler monitoringHandler = monitoringDeployment.start();

        server = Undertow.builder()
                .addHttpListener(port, "0.0.0.0")
                .setHandler(
                        Handlers.path().addExactPath(HEALTH_PATH, new HealthHandler())
                                .addExactPath(MONITORING_PATH, monitoringHandler))
                .build();

        server.start();
    }

    public void stop() {
        server.stop();
    }

}
