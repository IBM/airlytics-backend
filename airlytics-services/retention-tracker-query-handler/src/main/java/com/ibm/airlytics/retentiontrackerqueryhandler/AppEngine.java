package com.ibm.airlytics.retentiontrackerqueryhandler;

import com.ibm.airlock.common.data.Feature;
import com.ibm.airlytics.retentiontracker.airlock.AirlockConstants;
import com.ibm.airlytics.retentiontrackerqueryhandler.airlock.Airlock;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.DbExporter;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.DbHandler;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.ParquetDumper;
import com.ibm.airlytics.retentiontrackerqueryhandler.dsr.DSRRequestsScrapper;
import com.ibm.airlytics.retentiontrackerqueryhandler.polls.PollResultsAggregator;
import com.ibm.airlytics.retentiontrackerqueryhandler.utils.ConfigurationManager;
import com.ibm.airlytics.retentiontracker.airlock.AirlockManager;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import com.ibm.airlytics.retentiontrackerqueryhandler.consumer.QueueListener;
import com.ibm.airlytics.retentiontracker.exception.TrackerInitializationException;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.split.ParquetSplitter;
import com.ibm.airlytics.retentiontrackerqueryhandler.publisher.QueryQueuePublisher;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.ibm.airlytics.retentiontrackerqueryhandler.scheduling.ScheduleTaskService;

import javax.annotation.PostConstruct;
import java.io.IOException;

@Component
public class AppEngine {
    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(AppEngine.class.getName());
    private final static String APP_VER = "1.1";
    private final QueueListener queueListener;
    private final QueryQueuePublisher queuePublisher;
    private final DbHandler dbHandler;
    private final DbExporter dbExporter;
    private final Environment env;
    private final ParquetDumper parquetDumber;
    private final ParquetSplitter parquetSplitter;
    private final ScheduleTaskService scheduleTaskService;
    private final Airlock airlock;
    private final DSRRequestsScrapper dsrRequestsScrapper;
    private final ConfigurationManager configurationManager;
    private final PollResultsAggregator pollResultsAggregator;

    public final static String TYPE_VARIABLE = "USERDB_PROCESS_TYPE";
    public final static String TYPE_RETENTION = "retention";
    public final static String TYPE_BOUNCER = "bouncer";
    private final static String TYPE_DUMPER = "dumper";
    private final static String TYPE_DUMPER_COMPLETER = "dumper-completer";
    private final static String TYPE_SPLITTER = "splitter";
    private final static String TYPE_PRUNER = "pruner";
    private final static String TYPE_DELETER = "deleter";
    private final static String TYPE_SCRAPPER = "scrapper";
    private final static String TYPE_POLLS = "polls";

    public AppEngine(QueueListener queueListener, QueryQueuePublisher queuePublisher, DbHandler dbHandler, DbExporter dbExporter, Environment env, ParquetDumper parquetDumber, ParquetSplitter parquetSplitter, ScheduleTaskService scheduleTaskService, Airlock airlock, DSRRequestsScrapper dsrRequestsScrapper, ConfigurationManager configurationManager, PollResultsAggregator pollResultsAggregator) {
        this.queueListener = queueListener;
        this.queuePublisher = queuePublisher;
        this.dbHandler = dbHandler;
        this.dbExporter = dbExporter;
        this.env = env;
        this.parquetDumber = parquetDumber;
        this.parquetSplitter = parquetSplitter;
        this.scheduleTaskService = scheduleTaskService;
        this.airlock = airlock;
        this.dsrRequestsScrapper = dsrRequestsScrapper;
        this.configurationManager = configurationManager;
        this.pollResultsAggregator = pollResultsAggregator;
    }

    @PostConstruct
    public void process() throws IOException, TrackerInitializationException, InterruptedException {
        String appType = env.getProperty(TYPE_VARIABLE, TYPE_RETENTION);
        logger.info("appType:"+env.getProperty(TYPE_VARIABLE));
        logger.info("ACHSBAR");
        logger.info("application version is:"+APP_VER);
        if (appType.equals(TYPE_RETENTION)) {
            logger.info("Running as retention query handler");
            //query for non active users
            queuePublisher.connectToRabbitMQ();
//            queueListener.registerForWork(null);
            dbHandler.listenForNonActiveUsers(configurationManager.getTrackerInitialDelay(), configurationManager.getTrackerDelay());
        } else if (appType.equals(TYPE_BOUNCER)) {
            //listen to bounced tokens
            logger.info("Running as retention bounce listener");
            queueListener.registerForWork(null);
        } else if (appType.equals(TYPE_DUMPER)) {
            // be a parquet dumber
            logger.info("Running as DB dumper");
            Runnable dumperTask = parquetDumber.getDumpDBProcess();
            String schedulingRule = getDumpDbSchedulingRule();
            scheduleTaskService.addTaskToScheduler(TYPE_DUMPER, dumperTask, schedulingRule);

            Runnable completerTask = parquetDumber.getDumpDBCompleterProcess();
            String cSchedulingRule = getDumpDbCompleterSchedulingRule();

            scheduleTaskService.addTaskToScheduler(TYPE_DUMPER_COMPLETER, completerTask, cSchedulingRule);
        } else if (appType.equals(TYPE_DUMPER_COMPLETER)) {

            parquetDumber.dumpDB("2022-01-15","s3://airlytics-exports/dbdump/userdb-snapshot-auto-2022-01-15/");
            parquetDumber.dumpDB("2022-01-16","s3://airlytics-exports/dbdump/userdb-snapshot-auto-2022-01-16/");
            parquetDumber.dumpDB("2022-01-17","s3://airlytics-exports/dbdump/userdb-snapshot-auto-2022-01-17/");
            parquetDumber.dumpDB("2022-01-18","s3://airlytics-exports/dbdump/userdb-snapshot-auto-2022-01-18/");
            parquetDumber.dumpDB("2022-01-19","s3://airlytics-exports/dbdump/userdb-snapshot-auto-2022-01-19/");
            parquetDumber.dumpDB("2022-01-20","s3://airlytics-exports/dbdump/userdb-snapshot-auto-2022-01-20/");
        } else if (appType.equals(TYPE_SPLITTER)) {
            // be a parquet splitter
            logger.info("Running as DB splitter");
//            Runnable dumperTask = parquetDumber.getDumpDBProcess();
//            String schedulingRule = getDumpDbSchedulingRule();
//            scheduleTaskService.addTaskToScheduler(TYPE_DUMPER, dumperTask, schedulingRule);
//            parquetSplitter.splitDB("2021-04-08");
            parquetSplitter.startSplitting();
        } else if (appType.equals(TYPE_PRUNER)) {
            logger.info("Running as DB pruner");
            Runnable pruneTask = dbHandler.getPruneProcess();
            String schedulingRule = getPrunerSchedulingRule();
            scheduleTaskService.addTaskToScheduler(TYPE_PRUNER, pruneTask, schedulingRule);
        } else if (appType.equals(TYPE_DELETER)) {
            logger.info("Running as Users Deleter");
        } else if (appType.equals(TYPE_SCRAPPER)) {
            logger.info("Running as DSR Scrapper");
            dsrRequestsScrapper.configure();
            Runnable scrapTask = dsrRequestsScrapper.getScrapperTask();
            String schedulingRule = getScrapperSchedulingRule();
            scheduleTaskService.addTaskToScheduler(TYPE_SCRAPPER, scrapTask, schedulingRule);
        } else if (appType.equals(TYPE_POLLS)) {
            logger.info("starting polls");
            pollResultsAggregator.start();
        }
    }

    @Scheduled(initialDelay = 600000,fixedDelay = 600000)
    private void refreshAirlock() {
        if (airlock.refreshAirlock()) {
            updateSchedulingRules();
            if (env.getProperty(TYPE_VARIABLE, TYPE_RETENTION).equals(TYPE_POLLS)) {
                pollResultsAggregator.updateScheduler();
            }
        }
    }

    private void updateSchedulingRules() {
        String appType = env.getProperty(TYPE_VARIABLE, TYPE_RETENTION);
        if (appType.equals(TYPE_DUMPER)) {
            Runnable dumperTask = parquetDumber.getDumpDBProcess();
            String schedulingRule = getDumpDbSchedulingRule();
            scheduleTaskService.updateSchedulingTask(TYPE_DUMPER, dumperTask, schedulingRule);
            Runnable completerTask = parquetDumber.getDumpDBCompleterProcess();
            String cSchedulingRule = getDumpDbCompleterSchedulingRule();
            scheduleTaskService.updateSchedulingTask(TYPE_DUMPER_COMPLETER, completerTask, cSchedulingRule);
        } else if (appType.equals(TYPE_PRUNER)) {
            Runnable pruneTask = dbHandler.getPruneProcess();
            String schedulingRule = getPrunerSchedulingRule();
            scheduleTaskService.updateSchedulingTask(TYPE_PRUNER, pruneTask, schedulingRule);
        } else if (appType.equals(TYPE_SCRAPPER)) {
            Runnable scrapTask = dsrRequestsScrapper.getScrapperTask();
            String schedulingRule = getScrapperSchedulingRule();
            scheduleTaskService.updateSchedulingTask(TYPE_SCRAPPER, scrapTask, schedulingRule);
        }
    }

    private String getDumpDbSchedulingRule() {
        Feature dumper = AirlockManager.getInstance().getFeature(AirlockConstants.query.DB_DUMPER);
        return dumper.getConfiguration().getString("schedulingRule");
    }

    private String getDumpDbCompleterSchedulingRule() {
        Feature dumper = AirlockManager.getInstance().getFeature(AirlockConstants.query.DB_DUMPER);
        return dumper.getConfiguration().getString("completerSchedulingRule");
    }
    private String getPrunerSchedulingRule() {
        Feature pruner = AirlockManager.getInstance().getFeature(AirlockConstants.query.DB_PRUNER);
        return pruner.getConfiguration().getString("schedulingRule");
    }

    private String getScrapperSchedulingRule() {
        Feature pruner = AirlockManager.getInstance().getFeature(AirlockConstants.DSR.DSR_SCRAPPER);
        return pruner.getConfiguration().getString("schedulingRule");
    }

}
