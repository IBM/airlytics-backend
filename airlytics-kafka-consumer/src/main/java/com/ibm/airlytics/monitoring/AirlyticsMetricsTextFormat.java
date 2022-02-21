package com.ibm.airlytics.monitoring;

import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import io.prometheus.client.Collector;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Enumeration;

public class AirlyticsMetricsTextFormat {

    public final static String JVM_METRICS_PREFIX = "jvm_";
    public final static String PROCESS_METRICS_PREFIX = "process_";

    /**
     * Write out the text version 0.0.4 of the given MetricFamilySamples.
     */
    public static void write(Writer writer, Enumeration<Collector.MetricFamilySamples> mfs, String productName) throws IOException {
        while (mfs.hasMoreElements()) {
            Collector.MetricFamilySamples metricFamilySamples = mfs.nextElement();
            writer.write("# HELP ");
            writer.write(metricFamilySamples.name);
            writer.write(' ');
            writeEscapedHelp(writer, metricFamilySamples.help);
            writer.write('\n');

            writer.write("# TYPE ");
            writer.write(metricFamilySamples.name);
            writer.write(' ');
            writer.write(typeString(metricFamilySamples.type));
            writer.write('\n');

            for (Collector.MetricFamilySamples.Sample sample : metricFamilySamples.samples) {
                writer.write(sample.name);
                if (sample.name.startsWith(JVM_METRICS_PREFIX) || sample.name.startsWith(PROCESS_METRICS_PREFIX)) {
                    // add extra label "product"
                    ArrayList extendedLabelNamesWithProductLabel = new ArrayList<>(sample.labelNames);
                    ArrayList extendedLabelValuesWithProductLabel = new ArrayList<>(sample.labelValues);
                    extendedLabelNamesWithProductLabel.add(AirlyticsConsumerConstants.PRODUCT);
                    extendedLabelValuesWithProductLabel.add(productName);
                    // copy the old metrics with new label and label values lists
                    sample = new Collector.MetricFamilySamples.Sample(sample.name, extendedLabelNamesWithProductLabel,
                            extendedLabelValuesWithProductLabel, sample.value, sample.timestampMs);
                }
                if (sample.labelNames.size() > 0) {
                    writer.write('{');
                    for (int i = 0; i < sample.labelNames.size(); ++i) {
                        writer.write(sample.labelNames.get(i));
                        writer.write("=\"");
                        writeEscapedLabelValue(writer, sample.labelValues.get(i));
                        writer.write("\",");
                    }
                    writer.write('}');
                }
                writer.write(' ');
                writer.write(Collector.doubleToGoString(sample.value));
                if (sample.timestampMs != null) {
                    writer.write(' ');
                    writer.write(sample.timestampMs.toString());
                }
                writer.write('\n');
            }
        }
    }

    private static void writeEscapedHelp(Writer writer, String s) throws IOException {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\':
                    writer.append("\\\\");
                    break;
                case '\n':
                    writer.append("\\n");
                    break;
                default:
                    writer.append(c);
            }
        }
    }

    private static void writeEscapedLabelValue(Writer writer, String s) throws IOException {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\':
                    writer.append("\\\\");
                    break;
                case '\"':
                    writer.append("\\\"");
                    break;
                case '\n':
                    writer.append("\\n");
                    break;
                default:
                    writer.append(c);
            }
        }
    }

    private static String typeString(Collector.Type t) {
        switch (t) {
            case GAUGE:
                return "gauge";
            case COUNTER:
                return "counter";
            case SUMMARY:
                return "summary";
            case HISTOGRAM:
                return "histogram";
            default:
                return "untyped";
        }
    }
}
