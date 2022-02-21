package com.ibm.airlytics.utilities.validationService.queryservice.health;

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
import java.util.ArrayList;
import java.util.List;

import static io.undertow.util.StatusCodes.OK;
import static io.undertow.util.StatusCodes.SERVICE_UNAVAILABLE;

public abstract class MonitoringServer {
    protected abstract List<HealthCheckable> getHealthComponents();

    private static final String MONITORING_PATH = "/metrics";
    private static final String HEALTH_PATH = "/healthcheck";


    private class HealthHandler implements HttpHandler {

        @Override
        public void handleRequest(HttpServerExchange exchange) throws Exception {
            String firstGoodResult = null;
            String resMessage;
            boolean isHealthy = true;
            ArrayList<String> problems = new ArrayList<>();
            for (HealthCheckable comp : getHealthComponents()) {
                if (!comp.isHealthy()) {
                    isHealthy = false;
                    problems.add(comp.getHealthMessage());
                } else if (firstGoodResult == null) {
                    firstGoodResult = comp.getHealthMessage();
                }
            }

            if (isHealthy) {
                exchange.setStatusCode(OK);
                resMessage = firstGoodResult;
            } else {
                exchange.setStatusCode(SERVICE_UNAVAILABLE);
                resMessage = problems.toString();
            }

            exchange.getResponseHeaders().add(Headers.CONTENT_TYPE, "text/plain");
            exchange.getResponseSender().send(resMessage);
        }
    }

    public void startServer(int port) throws ServletException {

        // Initialize default JVM metrics for Prometheus
        DefaultExports.initialize();

        DeploymentInfo monitoringServletInfo = Servlets.deployment()
                .setClassLoader(MonitoringServer.class.getClassLoader())
                .setContextPath("/")
                .setDeploymentName("monitoring")
                .addServlet(Servlets.servlet(MetricsServlet.class).addMapping("/*"));

        DeploymentManager monitoringDeployment = Servlets.defaultContainer().addDeployment(monitoringServletInfo);
        monitoringDeployment.deploy();
        HttpHandler monitoringHandler = monitoringDeployment.start();

        Undertow server = Undertow.builder()
                .addHttpListener(port, "0.0.0.0")
                .setHandler(
                        Handlers.path().addExactPath(HEALTH_PATH, new HealthHandler())
                                .addExactPath(MONITORING_PATH, monitoringHandler))
                .build();

        server.start();
    }
}
