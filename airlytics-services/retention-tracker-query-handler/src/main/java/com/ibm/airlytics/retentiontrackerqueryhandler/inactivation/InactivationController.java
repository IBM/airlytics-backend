package com.ibm.airlytics.retentiontrackerqueryhandler.inactivation;

import com.ibm.airlytics.retentiontrackerqueryhandler.db.DbHandler;
import com.ibm.airlytics.retentiontrackerqueryhandler.events.UninstallDetectedEvent;
import com.ibm.airlytics.retentiontrackerqueryhandler.utils.ConfigurationManager;
import com.ibm.airlytics.retentiontracker.Constants;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import com.ibm.airlytics.retentiontracker.model.User;
import io.prometheus.client.Counter;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

@Component
@DependsOn({"ConfigurationManager", "Airlock"})
public class InactivationController {

    static final Counter recievedBouncedMessageCounter = Counter.build()
            .name("retention_tracker_bounced_messages_total")
            .help("Total number of bounced messages received from queue.")
            .labelNames(Constants.PLATFORM, Constants.ENV)
            .register();
    static final Counter recievedBouncedTokensCounter = Counter.build()
            .name("retention_tracker_bounced_tokens_total")
            .help("Total number of bounced tokens received from queue.")
            .labelNames(Constants.PLATFORM, Constants.ENV)
            .register();

    final DbHandler dbHandler;
    final EventsSender eventsSender;
    private ThreadPoolExecutor executor;
    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(InactivationController.class.getName());
    public InactivationController(DbHandler dbHandler, EventsSender eventsSender, ConfigurationManager configurationManager) {
        this.dbHandler = dbHandler;
        this.eventsSender = eventsSender;
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(configurationManager.getInactivateThreads());
    }

    public void startUserInactivation(List<String> tokens) {
        inactivate(tokens);

    }

    private void inactivate(List<String> tokens) {
        logger.debug("inactivate:"+tokens.size()+" tokens");
        List<User> users = null;
        try {
            users = dbHandler.getInactiveUsers(tokens);
            List<UninstallDetectedEvent> events = new LinkedList<>();
            Date time = new Date();
            String platform = null; //assuming all tokens are from same platform
            List<String> ids = new ArrayList<>();
            for (User user : users) {
                UninstallDetectedEvent event = getEventFromUser(user, time);
                events.add(event);
                ids.add(user.getUserId());
                if (platform == null) {
                    platform = user.getPlatform();
                }
            }
            if (platform == null) {
                platform = "ios"; //can only happen in iOS
            }
            recievedBouncedTokensCounter.labels(platform, ConfigurationManager.getEnvVar()).inc(tokens.size());
            recievedBouncedMessageCounter.labels(platform, ConfigurationManager.getEnvVar()).inc();
            if (ids.size() > 0) {

                dbHandler.setUsersInactiveById(ids, platform);
            }
            if (events.size() > 0) {
                eventsSender.sendInactivationEvents(events);
            }

        } catch (SQLException | ClassNotFoundException e) {
            logger.error("failed inactivating users:"+e.getMessage());
            e.printStackTrace();
        }

    }

    class InactivationTask implements Runnable {
        List<String> tokens;
        public InactivationTask(List<String> tokens) {
            this.tokens = tokens;
        }

        public void run() {
            logger.debug("inactivateUsers:"+tokens.size()+" tokens");
            List<User> users = null;
            try {
                users = dbHandler.getInactiveUsers(tokens);
                List<UninstallDetectedEvent> events = new LinkedList<>();
                Date time = new Date();
                String platform = null; //assuming all tokens are from same platform
                List<String> ids = new ArrayList<>();
                for (User user : users) {
                    UninstallDetectedEvent event = getEventFromUser(user, time);
                    events.add(event);
                    ids.add(user.getUserId());
                    if (platform == null) {
                        platform = user.getPlatform();
                    }
                }

                if (ids.size() > 0) {
                    dbHandler.setUsersInactiveById(ids, platform);
//                    dbHandler.inactivateUsers(realTokens, platform);
                }
                if (events.size() > 0) {
//                    eventsSender.sendInactivationEvents(events);
                }

            } catch (SQLException | ClassNotFoundException e) {
                logger.error("failed inactivating users:"+e.getMessage());
                e.printStackTrace();
            }

        }
    }

    private UninstallDetectedEvent getEventFromUser(User user, Date time) {
        UninstallDetectedEvent event = new UninstallDetectedEvent();
        event.setUserId(user.getUserId());
        event.setEventTime(time.getTime());
        event.setPlatform(user.getPlatform());
        event.setProductId(user.getProductId());
        event.setSchemaVersion("1.0");
        event.setEventId(getUUID());
        return event;
    }
    private String getUUID() {
        UUID uuid = UUID.randomUUID();
        return uuid.toString();
    }
}
