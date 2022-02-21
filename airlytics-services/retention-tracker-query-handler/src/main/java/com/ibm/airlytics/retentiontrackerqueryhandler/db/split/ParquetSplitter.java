package com.ibm.airlytics.retentiontrackerqueryhandler.db.split;

import com.ibm.airlytics.retentiontrackerqueryhandler.db.DbHandler;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.PersistenceConsumerConstants;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.athena.AthenaHandler;
import com.ibm.airlytics.retentiontracker.Constants;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import com.ibm.airlytics.retentiontracker.utils.TrackerTimer;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.*;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

@Component
@DependsOn("Airlock")
public class ParquetSplitter {
    static final Counter recordsProcessedCounter = Counter.build()
            .name("parquet_splitter_records_processed_total")
            .help("Total records processed by the persistence consumer.")
            .labelNames("result").register();

    //Number of write trials. Includes both fail and success writes.
    static final Counter filesWrittenCounter = Counter.build()
            .name("parquet_splitter_files_written_total")
            .help("Total files written by the persistence consumer.")
            .labelNames("result").register();


    static final Summary filesSizeBytesSummary = Summary.build()
            .name("parquet_splitter_file_size_bytes")
            .help("File size in bytes.").register();

    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(ParquetSplitter.class.getName());
    private final DbHandler dbHandler;
    private final AthenaHandler athenaHandler;
    private final SplitCursor splitCursor;
    private PersistenceSplitConfig config;

    private String awsSecret;
    private String awsKey;
    private String s3FilePrefix;
    private long totalRecords = 0;
    private long recordCounter = 0;
    private Instant dumpIntervalStart;
    private String parquetSchema;
    private String writeErrorMsg = "";
    private ThreadPoolExecutor executor;  //parallelize shards writing to S3
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    public ParquetSplitter(DbHandler dbHandler, AthenaHandler athenaHandler, SplitCursor splitCursor) throws IOException {
        this.dbHandler = dbHandler;
        this.athenaHandler = athenaHandler;
        this.splitCursor = splitCursor;
        config = new PersistenceSplitConfig();
        config.initWithAirlock();
        //S3 configuration
        this.awsSecret = System.getenv(PersistenceConsumerConstants.AWS_ACCESS_SECRET_PARAM); //if set as environment parameters use them otherwise will be taken from role
        this.awsKey = System.getenv(PersistenceConsumerConstants.AWS_ACCESS_KEY_PARAM); //if set as environment parameters use them otherwise will be taken from role
        this.s3FilePrefix = config.getS3Bucket() + File.separator + config.getS3RootFolder() + File.separator;

        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(config.getTableWriteThreads());
        dumpIntervalStart = Instant.now();
    }

    public Runnable getDumpDBProcess() {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    splitDB();
                } catch (IOException | InterruptedException e) {
                    logger.error("error in dumpDB process:"+e.getMessage());
                    e.printStackTrace();
                }
            }
        };
    }
    public void dumpPeriodically() {
        logger.info("dumpPeriodically");
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    splitDB();
                } catch (IOException | InterruptedException e) {
                    logger.error("error in dumpDB process:"+e.getMessage());
                    e.printStackTrace();
                }
            }
        },0, 1, TimeUnit.DAYS);
    }
    public void splitDB() throws IOException, InterruptedException {
        logger.info("Running dumpDB process");
        String day = dateFormat.format(new Date());
        splitDB(day);
    }
    public void splitDB(String day) throws IOException, InterruptedException {
        logger.info("Running plitDB process for day:"+day);
        TrackerTimer timer = new TrackerTimer();
        //get all maps configs
        List<TableSplitConfig> tableConfigs = config.getTables();
        //iterate over all relevant tables
//        String day = dateFormat.format(new Date());
//        day = "2021-01-28"; ////DELETE ME
//        day = "2021-02-23";
//        String snapshotPath = dbExporter.exportSnapshot(day);
        String snapshotPath = "s3://airlytics-exports/dbdump/userdb-snapshot-auto-2021-02-09/";
        if (snapshotPath==null) return;
        ArrayList<Future<TableDumpTask.Result>> writings = new ArrayList<Future<TableDumpTask.Result>>();
        for (TableSplitConfig tableConfig : tableConfigs) {
            TableSplitter tableDumper = new TableSplitter(tableConfig, dbHandler, athenaHandler);
            String dayPath = tableConfig.getLatestOnly() ? Constants.LATEST : day;
            TableDumpTask task = new TableDumpTask(tableDumper, dayPath, snapshotPath);
            Future<TableDumpTask.Result> future = executor.submit(task);
            writings.add(future);
        }
        logger.debug("writing " + writings.size() + " tables.");
        ArrayList<String> errors = new ArrayList<>();
        for (Future<TableDumpTask.Result> item : writings)
        {
            try {
                TableDumpTask.Result result = item.get();
                logger.debug("finished reading results of:"+result.filePath);
                if (result.error != null) {
                    errors.add(result.filePath + ": " + result.error);
                    writeErrorMsg = "Fail writing parquet file '" + result.filePath + "' to S3: " + result.error;
                }
            }
            catch (Exception e) {
                errors.add(e.toString());
            }
        }
        if (!errors.isEmpty()) {
            logger.error("errors during writing tables: " + errors.toString());
            return;
        }
        logger.info("finished splitDB for day "+day+" in "+timer.getTotalTime()/60.0+" minutes");
    }

    public void startSplitting() {
        try {
            String day = saveDayAndGetNext(null);
            while (day != null) {
                Instant now = Instant.now();
                int hour = now.atZone(ZoneOffset.UTC).getHour();
                if (hour >= 11 && hour <= 12) {
                    //sleep 45 minutes
                    Thread.sleep(1*45*60*1000);
                } else {
                    splitDB(day);
                    day = saveDayAndGetNext(day);
                    //spleep 1 minute
                    Thread.sleep(1*60*1000);
                }
            }
            logger.info("FINISHED splitting DB.");
        } catch (InterruptedException | IOException | ParseException e) {
            e.printStackTrace();
            logger.error("error splitting:"+e.getMessage());
        }


    }

    private String saveDayAndGetNext(String day) throws IOException, ParseException {
        logger.info("returning next day of:"+day);
        String MIN_DAY = "2020-04-13";
        String MAX_DAY = "2021-05-15";
        if (day!=null) {
            this.splitCursor.setCursor(day);
        } else {
            day = splitCursor.getCursor();
            if (day==null || day.isEmpty()) {
                day = MIN_DAY;
            }
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(dateFormat.parse(day));// all done
        cal.add(Calendar.DATE, 1);
        Calendar calMax = Calendar.getInstance();
        calMax.setTime(dateFormat.parse(MAX_DAY));// all done
        if (cal.after(calMax)) {
            logger.info("reached max days");
            return null;
        }
        logger.info("returning next day of:"+day+". the next day is:"+dateFormat.format(cal.getTime()));
        return dateFormat.format(cal.getTime());
    }


    class TableDumpTask implements Callable {
        class Result {
            String error = null;
            String filePath;
        }

        TableSplitter tableDumper;
        String day;
        String snapshotPath;

        public TableDumpTask(TableSplitter tableDumper, String day, String snapshotPath) {
            this.tableDumper = tableDumper;
            this.day = day;
            this.snapshotPath = snapshotPath;
        }

        public Result call() throws SQLException, ClassNotFoundException, InterruptedException {
            logger.info("start creation of table from parquet:"+tableDumper.tableConfig.getTableName());
            Result res = new Result();
            TableSplitter.TableDumpResult result = tableDumper.dumpTable(day, snapshotPath);
            res.error = result.error;
            res.filePath = result.filepath;
            return res;
        }
    }
}
