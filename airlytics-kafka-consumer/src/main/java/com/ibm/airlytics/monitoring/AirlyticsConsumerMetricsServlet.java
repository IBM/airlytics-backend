package com.ibm.airlytics.monitoring;

import com.ibm.airlytics.airlock.AirlockManager;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.exporter.common.TextFormat;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.Writer;
import java.util.*;

public class AirlyticsConsumerMetricsServlet extends MetricsServlet {


    private CollectorRegistry registry;

    /**
     * Construct a MetricsServlet for the default registry.
     */
    public AirlyticsConsumerMetricsServlet() {
        this(CollectorRegistry.defaultRegistry);
    }

    /**
     * Construct a MetricsServlet for the given registry.
     *
     * @param registry collector registry
     */
    public AirlyticsConsumerMetricsServlet(CollectorRegistry registry) {
        this.registry = registry;
    }

    @Override
    protected void doGet(final HttpServletRequest req, final HttpServletResponse resp)
            throws IOException {
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setContentType(TextFormat.CONTENT_TYPE_004);

        Writer writer = resp.getWriter();
        try {
            AirlyticsMetricsTextFormat.write(writer, registry.filteredMetricFamilySamples(parse(req)), AirlockManager.getProduct());
            writer.flush();
        } finally {
            writer.close();
        }
    }

    private Set<String> parse(HttpServletRequest req) {
        String[] includedParam = req.getParameterValues("name[]");
        if (includedParam == null) {
            return Collections.emptySet();
        } else {
            return new HashSet<String>(Arrays.asList(includedParam));
        }
    }
}
