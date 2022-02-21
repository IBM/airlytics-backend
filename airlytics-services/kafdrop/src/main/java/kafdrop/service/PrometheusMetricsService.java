package kafdrop.service;

import io.prometheus.client.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
public class PrometheusMetricsService {
    private static final Logger LOG = LoggerFactory.getLogger(PrometheusMetricsService.class);

    private static final Gauge topicTotalMessages = Gauge.build()
            .name("airlytics_kafka_topic_total_messages")
            .help("Topic message gouge.")
            .labelNames("topic")
            .register();

    private static final Gauge topicAvailableMessages = Gauge.build()
            .name("airlytics_kafka_topic_available_messages")
            .help("Topic available message gauge")
            .labelNames("topic")
            .register();

    private static final Gauge topicConsumerLags = Gauge.build()
            .name("airlytics_kafka_topic_lags")
            .help("Topic consumer group combined lags")
            .labelNames("topic", "consumer")
            .register();

    private final KafkaMonitor kafkaMonitor;

    public PrometheusMetricsService(KafkaMonitor kafkaMonitor) {
        this.kafkaMonitor = kafkaMonitor;
    }

    @PostConstruct
    public void init() {
        LOG.info("PrometheusMetricsService instantiated");
    }

    private static boolean firstRun = true;

    @Scheduled(initialDelay = 10_000, fixedRate = 10_000)
    public void processStream() {

        if(firstRun) {
            LOG.info("PrometheusMetricsService scheduled");
        }
        final var topics = kafkaMonitor.getTopics();
        final var consumers = kafkaMonitor.getConsumers(topics);

        if(firstRun) {
            LOG.info("PrometheusMetricsService found " + topics.size() + " topics and " + consumers.size() + " consumer groups");
        }

        topics.forEach(topic -> {
            topicTotalMessages.labels(topic.getName()).set(topic.getTotalSize());
            topicAvailableMessages.labels(topic.getName()).set(topic.getAvailableSize());
        });

        consumers.forEach(consumer -> {
            final var group = consumer.getGroupId();

            if(consumer.getTopics() != null) {
                consumer.getTopics().forEach(ct -> {
                    topicConsumerLags.labels(ct.getTopic(), group).set(ct.getLag());
                });
            }
        });
        if(firstRun) {
            LOG.info("PrometheusMetricsService first run finished");
            firstRun = false;
        }
    }
}
