package com.ibm.airlytics.retentiontrackerqueryhandler.db;

import com.ibm.airlytics.retentiontracker.Constants;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import com.ibm.airlytics.retentiontracker.utils.TrackerTimer;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.athena.AthenaHandler;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

import static com.ibm.airlytics.retentiontrackerqueryhandler.db.PersistenceConsumerConstants.*;

@Component
@DependsOn("Airlock")
public class ParquetDumper {
    static final Counter recordsProcessedCounter = Counter.build()
            .name("parquet_dumper_records_processed_total")
            .help("Total records processed by the persistence consumer.")
            .labelNames("result").register();

    //Number of write trials. Includes both fail and success writes.
    static final Counter filesWrittenCounter = Counter.build()
            .name("parquet_dumper_files_written_total")
            .help("Total files written by the persistence consumer.")
            .labelNames("result").register();


    static final Summary filesSizeBytesSummary = Summary.build()
            .name("parquet_dumper_file_size_bytes")
            .help("File size in bytes.").register();

    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(ParquetDumper.class.getName());
    private final DbHandler dbHandler;
    private final AthenaHandler athenaHandler;
    private final DbExporter dbExporter;
    private final DumpBookmark dumpBookmark;
    private PersistenceConsumerConfig config;
    private boolean dumpRunning = false;
    private boolean completerRunning = false;

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

    public ParquetDumper(DbHandler dbHandler, AthenaHandler athenaHandler, DbExporter dbExporter, DumpBookmark dumpBookmark) throws IOException {
        this.dbHandler = dbHandler;
        this.athenaHandler = athenaHandler;
        this.dbExporter = dbExporter;
        this.dumpBookmark = dumpBookmark;
        config = new PersistenceConsumerConfig();
        config.initWithAirlock();
        //S3 configuration
        this.awsSecret = System.getenv(AWS_ACCESS_SECRET_PARAM); //if set as environment parameters use them otherwise will be taken from role
        this.awsKey = System.getenv(AWS_ACCESS_KEY_PARAM); //if set as environment parameters use them otherwise will be taken from role
        this.s3FilePrefix = config.getS3Bucket() + File.separator + config.getS3RootFolder() + File.separator;

        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(config.getTableWriteThreads());
        dumpIntervalStart = Instant.now();
    }

    public Runnable getDumpDBProcess() {
        return new Runnable() {
            @Override
            public void run() {
                if (completerRunning) {
                    logger.error("DB COMPLETER is already running when trying to run dumper. running dumper anyway");
                }
                dumpRunning = true;
                try {
                    dumpDB();
                } catch (IOException | InterruptedException e) {
                    logger.error("error in dumpDB process:"+e.getMessage());
                    e.printStackTrace();
                } finally {
                    dumpRunning = false;
                    logger.info("tearing down dumper");
                    System.exit(0);
                }
            }
        };
    }

    public Runnable getDumpDBCompleterProcess() {
        return new Runnable() {
            @Override
            public void run() {
                if (dumpRunning) {
                    logger.error("DB DUMP is already running when trying to run completer");
                    return;
                }
                completerRunning = true;
                try {
                    completeDB();
                } catch (IOException | InterruptedException e) {
                    logger.error("error in dumpDB-completer process:"+e.getMessage());
                    e.printStackTrace();
                } finally {
                    logger.info("tearing down dumper completer");
                    completerRunning = false;
                    System.exit(0);
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
                    dumpDB();
                } catch (IOException | InterruptedException e) {
                    logger.error("error in dumpDB process:"+e.getMessage());
                    e.printStackTrace();
                }
            }
        },0, 1, TimeUnit.DAYS);
    }
    public void dumpDB() throws IOException, InterruptedException {
        logger.info("Running dumpDB process");
        String day = dateFormat.format(new Date());
        dumpDB(day);
    }

    public void completeDB() throws IOException, InterruptedException {
        logger.info("Running dumpDB process");
        String day = dateFormat.format(new Date());
        dumpDB(day, null, true);
    }

    public void dumpDB(String day) throws IOException, InterruptedException {
        this.dumpDB(day, null, false);
    }

    public void dumpDB(String day, String sPath) throws IOException, InterruptedException {
        this.dumpDB(day, sPath, false);
    }
    public void dumpDB(String day, String sPath, boolean forComplete) throws IOException, InterruptedException {
        logger.info("Running dumpDB process");
        TrackerTimer timer = new TrackerTimer();
        //get all maps configs
        List<TableDumpConfig> tableConfigs = config.getTables();
        //iterate over all relevant tables
//        String day = dateFormat.format(new Date());
//        day = "2021-01-28"; ////DELETE ME
//        day = "2021-02-23";
        String snapshotPath = (sPath!=null && !sPath.isEmpty())? sPath : dbExporter.exportSnapshot(day);
        boolean isForComplete = forComplete || (sPath!=null && !sPath.isEmpty());
        logger.info("snapshot path:"+snapshotPath);
//        String snapshotPath = "s3://airlytics-exports/dbdump/userdb-snapshot-auto-2021-11-16/";
        if (snapshotPath==null) return;
        ArrayList<TableDumpTask.Result> writings = new ArrayList<>();
        ArrayList<String> errors = new ArrayList<>();
        for (TableDumpConfig tableConfig : tableConfigs) {
            TableDumper tableDumper = new TableDumper(tableConfig, dbHandler, athenaHandler, isForComplete);
            if (forComplete || (sPath!=null && !sPath.isEmpty())) {
                tableDumper.setEXTRA_PREFIX("completions58/");
            }
            String dayPath = tableConfig.getLatestOnly() ? Constants.LATEST : day;
            TableDumpTask task = new TableDumpTask(tableDumper, dayPath, snapshotPath);
//            Future<TableDumpTask.Result> future = executor.submit(task);
            try {
                TableDumpTask.Result result = task.call();
                writings.add(result);
            } catch (SQLException | ClassNotFoundException e) {
                logger.error("error in writing table "+tableConfig.getSchemaName()+"."+tableConfig.getTableName()+":"+e.getMessage());
                errors.add(e.toString());
            }

        }
        logger.debug("writing " + writings.size() + " tables.");

        for (TableDumpTask.Result result : writings)
        {
            try {
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
        } else {
            try {
                writeBookmark(day);
            } catch (ParseException | IOException e) {
                e.printStackTrace();
                logger.error("error writing bookmark:"+e.getMessage());
            }
        }
        logger.info("finished dumpDB for day "+day+" in "+timer.getTotalTime()/60.0+" minutes");
    }

    private void writeBookmark(String day) throws IOException, ParseException {
        String lastBookmark = dumpBookmark.getCursor();
        if (lastBookmark==null || lastBookmark.isEmpty()) {
            dumpBookmark.setCursor(day);
        } else {
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date lastDate = dateFormat.parse(lastBookmark);
            Date thisDate = dateFormat.parse(day);
            if (thisDate.after(lastDate)) {
                dumpBookmark.setCursor(day);
            }
        }
    }

    class TableDumpTask implements Callable {
        class Result {
            String error = null;
            String filePath;
        }

        TableDumper tableDumper;
        String day;
        String snapshotPath;

        public TableDumpTask(TableDumper tableDumper, String day, String snapshotPath) {
            this.tableDumper = tableDumper;
            this.day = day;
            this.snapshotPath = snapshotPath;
        }

        public Result call() throws SQLException, ClassNotFoundException, InterruptedException, IOException {
            logger.info("start creation of table from parquet:"+tableDumper.tableConfig.getTableName());
            TableDumpTask.Result res = new TableDumpTask.Result();
            TableDumper.TableDumpResult result = null;
            try {
                result = tableDumper.dumpTable(day, snapshotPath);
            } catch (IOException | SQLException | ClassNotFoundException | InterruptedException e ) {
                e.printStackTrace();
                logger.error("error in dumping table:"+tableDumper.tableConfig.getTableName());
                throw e;
            }
            res.error = result.error;
            res.filePath = result.filepath;
            return res;
        }
    }
}
