package com.ibm.weather.airlytics.jobs.eventspatch.monitoring;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import io.prometheus.client.hotspot.DefaultExports;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Set;

@RestController
public class MetricsController {

    @Autowired
    private CollectorRegistry collectorRegistry;

    @PostConstruct
    public void init() {
        // Initialize default JVM metrics for Prometheus
        DefaultExports.initialize();
    }

    @GetMapping(path = "/metrics")
    public ResponseEntity<String> getMetrics(@RequestParam(value = "name[]", required = false, defaultValue = "") Set<String> name) {
        String result = writeRegistry(name);

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_TYPE, TextFormat.CONTENT_TYPE_004)
                .body(result);
    }

    private String writeRegistry(Set<String> metricsToInclude) {
        try {
            Writer writer = new StringWriter();
            TextFormat.write004(writer, collectorRegistry.filteredMetricFamilySamples(metricsToInclude));
            return writer.toString();
        } catch (IOException e) {
            // This actually never happens since StringWriter::write() doesn't throw any IOException
            throw new RuntimeException("Writing metrics failed", e);
        }
    }
}
