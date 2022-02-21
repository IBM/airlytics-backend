package com.ibm.airlytics.consumer.realTimeData;

import com.fasterxml.jackson.databind.JsonNode;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumer;
import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import com.ibm.airlytics.consumer.userdb.UserEvent;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RealTimeDataConsumer extends AirlyticsConsumer {

    static Map<String, CounterSetup> counterSetupMap;
    static final Counter eventsProcessedCounter = Counter.build()
            .name("airlytics_realtime_events_processed_total")
            .help("Total events processed by the realtime consumer.")
            .labelNames(AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();
    static final Counter eventsRecordedCounter = Counter.build()
            .name("airlytics_realtime_events_recorded_total")
            .help("Total events recorded to other counters by the realtime consumer.")
            .labelNames(AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();
    static final Counter eventsSkippedCounter = Counter.build()
            .name("airlytics_realtime_events_skipped_total")
            .help("Total events skipped by the realtime consumer.")
            .labelNames(AirlyticsConsumerConstants.REAL_TIME_SKIP_REASON,AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    private class CounterSetup {
        private Counter counter;

        public CounterSetup(Counter counter, Date createdTime, Date lastEncounteredTime, RealTimePrometheusConfig.EventConfig config) {
            this.counter = counter;
            this.isActive = true;
            this.createdTime = createdTime;
            this.lastEncounteredTime = lastEncounteredTime;
            this.config=config;
        }

        public boolean isActive() {
            return isActive;
        }

        boolean isActive;
        private Date createdTime;
        private Date lastEncounteredTime;
        private RealTimePrometheusConfig.EventConfig config;

        public Counter getCounter() {
            return counter;
        }

        public Date getCreatedTime() {
            return createdTime;
        }

        public void setCreatedTime(Date createdTime) {
            this.createdTime = createdTime;
        }

        public Date getLastEncounteredTime() {
            return lastEncounteredTime;
        }

        public void setLastEncounteredTime(Date lastEncounteredTime) {
            this.lastEncounteredTime = lastEncounteredTime;
        }

        public RealTimePrometheusConfig.EventConfig getConfig() {
            return config;
        }

        public void setConfig(RealTimePrometheusConfig.EventConfig config) {
            this.config = config;
        }
    }

    private static final Logger LOGGER = Logger.getLogger(com.ibm.airlytics.consumer.realTimeData.RealTimeDataConsumer.class.getName());
    private RealTimeDataConsumerConfig config;
    private RealTimeDataConsumerConfig newConfig = null;
    private long totalProgress = 0;
    private long lastProgress = 0;
    private Instant lastRecordProcessed = Instant.now();

    public RealTimeDataConsumer(RealTimeDataConsumerConfig config) {
        super(config);
        this.config = config;
        counterSetupMap = createCounterSetups(config.getEventsConfig().getEvents());

        //schedule periodical prune
        Long pruneTime = config.getPeriodicalPruneTimeMinutes();
        if (pruneTime > 0) {
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    pruneNonActiveCounters();
                }
            },pruneTime, pruneTime, TimeUnit.MINUTES);
        }

    }

    private synchronized void pruneNonActiveCounters() {
        LOGGER.info("pruneNonActiveCounters");
        for (Map.Entry<String, CounterSetup> entry : counterSetupMap.entrySet()) {
            String eventName = entry.getKey();
            CounterSetup counterSetup = entry.getValue();
            RealTimePrometheusConfig.EventConfig eventConfig = config.getEventsConfig().getEventConfig(eventName);
            long pruneThreshold = getPruneThreshold(eventConfig, config);
            if (pruneThreshold > 0) {
                long nonActiveTime = getNonActiveTime(counterSetup);
                if (nonActiveTime > pruneThreshold) {
                    deactivateCounter(eventName, counterSetup);
                }
            }
        }
    }

    private long getNonActiveTime(CounterSetup counterSetup) {
        Date lastDate = counterSetup.getCreatedTime();
        if (counterSetup.getLastEncounteredTime() != null) {
            lastDate = counterSetup.getLastEncounteredTime();
        }
        long nonActiveTimeMilli = new Date().getTime() - lastDate.getTime();
        //return in seconds
        return new Double(nonActiveTimeMilli/1000.0).longValue();
    }

    private long getPruneThreshold(RealTimePrometheusConfig.EventConfig eventConfig, RealTimeDataConsumerConfig config) {
        Long threshold = eventConfig.getStaleThresholdSeconds();
        if (threshold == null) {
            threshold = config.getStaleThresholdSeconds();
        }
        return threshold != null ? threshold : 0;
    }

    @Override
    protected int processRecords(ConsumerRecords<String, JsonNode> records) {
        if (records.isEmpty())
            return 0;

        LOGGER.debug("RealTime consumer - process records:" + records.count());

        updateToNewConfigIfExists();

        int recordsProcessed = 0;
        for (ConsumerRecord<String, JsonNode> record : records) {
            UserEvent event = null;
            String result = "success";
            try {
                event = new UserEvent(record);
            } catch (Exception e) {
                LOGGER.error("unrecognizable event:"+record+"."+e.getMessage());
                continue;
            }

            processEventIfNeeded(event);
            ++recordsProcessed;

        }

        commit();

        totalProgress += recordsProcessed;

        Instant now = Instant.now();
        Duration timePassed = Duration.between(lastRecordProcessed, now);
        if (timePassed.compareTo(Duration.ofSeconds(5)) >= 0) {
            LOGGER.debug("Processed " + totalProgress + " records. Current rate: " +
                    ((double) (totalProgress - lastProgress)) / timePassed.toMillis() * 1000 + " records/sec");
            lastRecordProcessed = now;
            lastProgress = totalProgress;
        }

        return recordsProcessed;
    }

    private Map<String, CounterSetup> createCounterSetups(Map<String, RealTimePrometheusConfig.EventConfig> eventsMap) {
        Map<String, CounterSetup> toRet = new HashMap<>();
        Date now = new Date();
        for (Map.Entry<String, RealTimePrometheusConfig.EventConfig> entry : eventsMap.entrySet()) {
            String eventName = entry.getKey();
            RealTimePrometheusConfig.EventConfig eventConfig = entry.getValue();
            List<String> attributes = eventConfig.getAttributes();
            String[] labels = getLabelsList(attributes);
            String counterName = getCounterName(eventName);
            String counterHelp = getCounterHelp(eventName);
            Counter counter = Counter.build()
                    .name(counterName)
                    .help(counterHelp)
                    .labelNames(labels).register();
            CounterSetup counterSetup = new CounterSetup(counter, now, null, eventConfig);
            toRet.put(eventName, counterSetup);
        }
        return toRet;
    }
    private CounterSetup createCounter(String eventName, RealTimePrometheusConfig.EventConfig eventConfig) {
        Date now = new Date();
        List<String> attributes = eventConfig.getAttributes();
        String[] labels = getLabelsList(attributes);
        String counterName = getCounterName(eventName);
        String counterHelp = getCounterHelp(eventName);
        Counter counter = Counter.build()
                .name(counterName)
                .help(counterHelp)
                .labelNames(labels).register();
        CounterSetup counterSetup = new CounterSetup(counter, now, null, eventConfig);
        return counterSetup;
    }
    private Map<String, Counter> createPrometheusCounters(Map<String, RealTimePrometheusConfig.EventConfig> eventsMap) {
        Map<String,Counter> toRet = new HashMap<>();
        for (Map.Entry<String, RealTimePrometheusConfig.EventConfig> entry : eventsMap.entrySet()) {
            String eventName = entry.getKey();
            List<String> attributes = entry.getValue().getAttributes();
            String[] labels = getLabelsList(attributes);
            String counterName = getCounterName(eventName);
            String counterHelp = getCounterHelp(eventName);
            Counter counter = Counter.build()
                    .name(counterName)
                    .help(counterHelp)
                    .labelNames(labels).register();
            toRet.put(eventName, counter);
        }
        return toRet;
    }

    private String getCounterHelp(String eventName) {
        return "Total events of type '"+eventName+"' processed";
    }

    private void removeCounter() {
        eventsProcessedCounter.remove();
    }
    private String getCounterName(String eventName) {
        String legalEventName = getLegalName(eventName);
        return "airlytics_realtime_event_"+legalEventName+"_total";
    }

    private String getLegalName(String eventName) {
        return eventName.replace("-", "_");
    }

    private String[] getLabelsList(List<String> attributes) {
        List<String> arr = new ArrayList<>(attributes);
        arr.add(AirlyticsConsumerConstants.ENV);
        arr.add(AirlyticsConsumerConstants.PRODUCT);
        return arr.stream()
                .toArray(String[]::new);
    }

    private void processEventIfNeeded(UserEvent event) {
        String eventName = event.getEventName();
        CounterSetup eventCounterSetup = counterSetupMap.get(eventName);
        if (eventCounterSetup != null) {
            if (!eventCounterSetup.isActive()) {
                reactivateCounter(eventName, eventCounterSetup);
            }
            Counter eventCounter = eventCounterSetup.getCounter();
            RealTimePrometheusConfig.EventConfig eventConfig = config.getEventsConfig().getEventConfig(eventName);
            if (eventConfig.getTimeThresholdSeconds() != null) {
                long threshold = eventConfig.getTimeThresholdSeconds();
                //get event time difference
                long now = new Date().getTime();
                long eventTime = event.getEventTime().getTime();
                double diff = (now - eventTime)/1000.0;
                LOGGER.debug("time difference is:"+diff+". threshold is:"+threshold);
                if (diff > threshold) {
                    LOGGER.debug("excluding event "+eventName+" with id:"+event.getEventId()+" because of time difference bigger than allowed");
                    eventsSkippedCounter.labels(AirlyticsConsumerConstants.REAL_TIME_SKIP_REASON_TIME, AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                    return;
                }
            }
            List<String> attributes = eventConfig.getAttributes();
            List<String> valuesList = new ArrayList<>();
            if (attributes != null) {
                for (String attribute : attributes) {
                    String attVal = AirlyticsConsumerConstants.REAL_TIME_ATTRIBUTE_UNKNOWN;
                    if (event.getAttributes() != null && nodeContainsField(event.getAttributes(), attribute)) {
                        attVal = event.getAttributes().get(attribute) != null ? event.getAttributes().get(attribute).asText() : AirlyticsConsumerConstants.REAL_TIME_ATTRIBUTE_NULL;
                    }
                    valuesList.add(attVal);
                }
            }
            valuesList.add(AirlockManager.getEnvVar());
            valuesList.add(AirlockManager.getProduct());
            String[] values = valuesList.stream().toArray(String[]::new);
            eventCounter.labels(values).inc();
            eventCounterSetup.setLastEncounteredTime(new Date());
            eventsProcessedCounter.labels(AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
            LOGGER.debug("incremented counter for "+eventName);
        } else {
            LOGGER.debug("event "+eventName+" is filtered out");
            eventsSkippedCounter.labels(AirlyticsConsumerConstants.REAL_TIME_SKIP_REASON_NAME, AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
        }
    }

    private void reactivateCounter(String eventName, CounterSetup eventCounterSetup) {
        RealTimePrometheusConfig.EventConfig eventConfig = config.getEventsConfig().getEventConfig(eventName);
        Counter counter = eventCounterSetup.getCounter();
        CollectorRegistry myRegistry = new CollectorRegistry();
        myRegistry.register(counter);
    }

    private void deactivateCounter(String eventName, CounterSetup eventCounterSetup) {
        LOGGER.info("deactivating eventName:"+eventName);
        RealTimePrometheusConfig.EventConfig eventConfig = config.getEventsConfig().getEventConfig(eventName);
        Counter counter = eventCounterSetup.getCounter();
        CollectorRegistry.defaultRegistry.unregister(counter);
    }

    private boolean nodeContainsField(JsonNode node, String field) {
        Iterator<Map.Entry<String, JsonNode>> nodes = node.fields();

        while (nodes.hasNext()) {
            Map.Entry<String, JsonNode> entry = nodes.next();
            if (entry.getKey().equals(field)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public synchronized void newConfigurationAvailable() {
        RealTimeDataConsumerConfig config = new RealTimeDataConsumerConfig();
        try {
            config.initWithAirlock();
            newConfig = config;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Note that this will NOT update Kafka URLs etc. If those change the consumer must be restarted.
    private synchronized void updateToNewConfigIfExists() {
        if (newConfig != null) {
            config = newConfig;
            newConfig = null;
            updatePrometheusCounters();
        }
    }

    private void updatePrometheusCounters() {
        for (Map.Entry<String, RealTimePrometheusConfig.EventConfig> entry : config.getEventsConfig().getEvents().entrySet()) {
            String eventName = entry.getKey();
            RealTimePrometheusConfig.EventConfig eventConfig = entry.getValue();
            if (counterSetupMap.get(eventName)!=null) {
                //this counter already exists, see if it changed
                CounterSetup oldCounterSetup = counterSetupMap.get(eventName);
                RealTimePrometheusConfig.EventConfig oldEventConfig = oldCounterSetup.getConfig();
                if (!oldEventConfig.allEquals(eventConfig)) {
                    //it was changed
                    if (oldEventConfig.attributesEquals(eventConfig)) {
                        //just the metadata was changed, no need to reset the counter
                        oldCounterSetup.setConfig(eventConfig);
                    } else {
                        //recreate the counter
                        deactivateCounter(eventName, oldCounterSetup);
                        CounterSetup newCounterSetup = createCounter(eventName, eventConfig);
                        counterSetupMap.put(eventName, newCounterSetup);
                    }
                }
            } else {
                //new counter
                CounterSetup counterSetup = createCounter(eventName, eventConfig);
                counterSetupMap.put(eventName, counterSetup);
            }
        }
    }
}
