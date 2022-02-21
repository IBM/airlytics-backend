package com.ibm.airlytics.retentiontrackerqueryhandler.polls;

import com.ibm.airlytics.retentiontrackerqueryhandler.airlock.AirlockClient;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.DbHandler;
import com.ibm.airlytics.retentiontrackerqueryhandler.utils.ConfigurationManager;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import org.json.JSONObject;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
@DependsOn({"ConfigurationManager","Airlock"})
public class PollResultsAggregator {

    private final DbHandler dbHandler;
    private final ConfigurationManager configurationManager;
    private PollResults pollResults;
    private ScheduledExecutorService scheduler;
    long initialDelay;
    long delay;
    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(PollResultsAggregator.class.getName());

    public PollResultsAggregator(DbHandler dbHandler, ConfigurationManager configurationManager) {
        this.dbHandler = dbHandler;
        this.configurationManager = configurationManager;
    }

    public void start() {
        logger.info("starting pollResultsAggregator");
        loadCache();
        initialDelay = configurationManager.getPollJobInitialDelay();
        delay = configurationManager.getPollJobdelay();
        scheduler = Executors.newScheduledThreadPool(1);
        runScheduler();
    }

    public void updateScheduler() {
        if (scheduler == null) {
            logger.error("scheduler is null exit updateScheduler");
            return;
        }

        long newDelay = configurationManager.getPollJobdelay();
        if (newDelay != delay) {
            delay = newDelay;
            scheduler.shutdown();
            runScheduler();
        }
    }

    private void runScheduler() {
        if (scheduler == null) {
            logger.error("scheduler is null exit runScheduler");
            return;
        }

        logger.info("starting pollResultsAggregator scheduler. initialDelay:"+initialDelay+". delay:"+delay);
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    aggregatePollResults();
                } catch (Exception e) {
                    logger.error("Failed in scrappePullData:"+e.getMessage());
                }
            }
        },initialDelay, delay, TimeUnit.MINUTES);
    }

    private void loadCache() {
        String productId = configurationManager.getAirLockInternalProductId();
        pollResults = new PollResults(productId);

        try {
            String cacheJSON = dbHandler.readPollCache();
            pollResults.load(cacheJSON);
        } catch (Exception e) {
            logger.error("Fail to read poll results cache: " + e.getMessage());
        }
    }

    private void aggregatePollResults() throws Exception {
        logger.info("aggregatePollResults");
        PollResults newPr = dbHandler.getPollsResults(pollResults.productId, pollResults.answersProcessedRows, pollResults.piAnswersProcessedRows);
        if (newPr.isEmpty()) {
            logger.info("did not find new results");
            JSONObject o = pollResults.toJSONObject();
            setResultsToAirlock(o);
        } else {
            newPr.add(pollResults);
            JSONObject o = newPr.toJSONObject();
            setResultsToAirlock(o);
            String resultsCache = newPr.toJSONString(o,true);
            dbHandler.updatePollCache(resultsCache);
            pollResults = newPr;
        }
        logger.info("polls result updated");
    }

    private void setResultsToAirlock(JSONObject res) {
        String resultsJSON = res.toString();
        AirlockClient client = new AirlockClient(configurationManager);
        client.setPollResults(resultsJSON);
    }
}


