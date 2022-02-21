package com.ibm.airlytics.retentiontrackerqueryhandler.inactivation;

import com.ibm.airlytics.retentiontrackerqueryhandler.db.DbHandler;
import com.ibm.airlytics.retentiontrackerqueryhandler.events.UninstallDetectedEvent;
import com.ibm.airlytics.retentiontrackerqueryhandler.rest.EventApiClient;
import com.ibm.airlytics.retentiontrackerqueryhandler.rest.EventApiException;
import com.ibm.airlytics.retentiontrackerqueryhandler.utils.ConfigurationManager;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;


@Component
public class EventsSender {
    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(EventsSender.class.getName());
    private ThreadPoolExecutor executor;
    private final ConfigurationManager configurationManager;

    public EventsSender(DbHandler dbHandler, ConfigurationManager configurationManager) {
        this.configurationManager = configurationManager;
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(20);
    }

    public void sendInactivationEvents(List<UninstallDetectedEvent> events) {
        if (events.size() <= 0) {
            logger.error("called sendInactivationEvents() with 0 events");
            return;
        }
        String apiKey = configurationManager.getEventsApiKey(events.get(0).getPlatform());
        EventApiClient client = new EventApiClient(configurationManager.getEventsProxyBaseURL(), configurationManager.getEventsProxyTrackPath(),
                apiKey, configurationManager.getEventApiRateLimit(), false, configurationManager.getEventApiRetries(), configurationManager.getEventApiBatchSize());
        RestAPICallingTask task = new RestAPICallingTask(events, client);
        executor.submit(task);
    }

    public class RestAPICallingTask implements Runnable {
        List<UninstallDetectedEvent> uninstallEvents;
        EventApiClient client;
        public RestAPICallingTask(List<UninstallDetectedEvent> uninstallEvents, EventApiClient client) {
            this.uninstallEvents = uninstallEvents;
            this.client = client;
        }
        @Override
        public void run() {
            try {
                this.client.sendEventsBatchToEventProxy(new ArrayList<>(this.uninstallEvents), 0, null);
            } catch (EventApiException e) {
                logger.error("error sending uninstall-events:"+e.getMessage());
                e.printStackTrace();
            }
        }

        private List<String> filterRejectedEvents(List<UninstallDetectedEvent> uninstallEvents, List<String> rejectedIds) {
            LinkedList<String> ids = new LinkedList<>();
            for (UninstallDetectedEvent event : uninstallEvents) {
                String eventId = event.getEventId();
                if (!rejectedIds.contains(eventId)) {
                    ids.add(event.getUserId());
                }
            }
            return ids;
        }
    }

}
