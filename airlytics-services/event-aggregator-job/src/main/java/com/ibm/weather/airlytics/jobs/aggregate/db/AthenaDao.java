package com.ibm.weather.airlytics.jobs.aggregate.db;

import com.ibm.weather.airlytics.common.airlock.AirlockException;
import com.ibm.weather.airlytics.common.athena.AthenaBasicDao;
import com.ibm.weather.airlytics.common.athena.AthenaDriver;
import com.ibm.weather.airlytics.jobs.aggregate.dto.EventAggregatorAirlockConfig;
import com.ibm.weather.airlytics.jobs.aggregate.dto.SecondaryAggregationConfig;
import com.ibm.weather.airlytics.jobs.aggregate.services.S3Service;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

@Repository
public class AthenaDao extends AthenaBasicDao {

    private static final Logger logger = LoggerFactory.getLogger(AthenaDao.class);

    @Autowired
    public AthenaDao(AthenaDriver athena) {
        super(athena);
    }

    public boolean isDateComplete(AthenaClient athenaClient, EventAggregatorAirlockConfig featureConfig, final LocalDate day)
            throws AthenaException, AirlockException {
        logger.info("Starting date complete query for {}", day);
        String query = buildDateCompletedQuery(featureConfig, day);
        final AtomicInteger cnt = new AtomicInteger(0);
        executeQueryProcessResult(
                athenaClient,
                featureConfig,
                query,
                (rows, meta) -> {

                    if(rows.size() == 2) {//0 is header
                        Row row = rows.get(1);
                        List<Datum> cols = row.data();

                        if(!cols.isEmpty()) {
                            Datum col = cols.get(0);

                            try {
                                Integer v = Integer.valueOf(col.varCharValue());
                                logger.info("Found {} partitions for day after {}", v, day);
                                cnt.set(v);
                            } catch (NumberFormatException e) {
                                logger.warn("Athena returned NaN: {}", col.varCharValue());
                            }
                        }
                    }

        });
        return (cnt.get() == PARTITIONS_NUMBER);
    }

    public int performDailyJob(AthenaClient athenaClient, EventAggregatorAirlockConfig featureConfig, LocalDate day, boolean isFirstRun)
            throws AthenaException, AirlockException {
        logger.info("Starting aggregation for {}", day);
        long ts = System.currentTimeMillis();
        int cnt = calculateDailyValues(athenaClient, featureConfig, day, ts);
        dropTempDailyTable(athenaClient, featureConfig, day, ts);

        if(isFirstRun) {
            createDailyTable(athenaClient, featureConfig);
        }
        return cnt;
    }

    public void performSecondaryAggregation(
            AthenaClient athenaClient,
            EventAggregatorAirlockConfig featureConfig,
            LocalDate day,
            SecondaryAggregationConfig aggregationConfig,
            boolean isFirstRun)
            throws AirlockException {
        logger.info("Starting aggregation of {} days back for {}", aggregationConfig.getAggregationWindowDays(), day);

        if(isFirstRun) {
            long ts = System.currentTimeMillis();
            calculateSecondaryValuesFirstRun(athenaClient, featureConfig, aggregationConfig, day, ts);
            dropTempSecondaryTable(athenaClient, featureConfig, aggregationConfig, day, ts);
        } else {
            final List<Future<Exception>> futures = new ArrayList<>(100);

            for(int partition = 0; partition < PARTITIONS_NUMBER; partition++) {
                futures.add(asyncCalculateSecondaryValues(athenaClient, featureConfig, aggregationConfig, day, partition));
            }

            for(Future<Exception> f : futures) {

                try {
                    Exception inner = f.get();

                    if(inner != null)  {
                        logger.warn("Aggregation failed", inner);

                        if(inner instanceof  RuntimeException) {
                            throw (RuntimeException)inner;
                        } else if(inner instanceof  AirlockException) {
                            throw (AirlockException)inner;
                        } else {
                            throw new AirlockException(inner);
                        }
                    }
                } catch (ExecutionException | InterruptedException outer) {
                    logger.warn("Aggregation interrupted", outer);
                    throw new AirlockException(outer);
                }
            }
            // table moves to the new date's folder
            dropSecondaryTable(athenaClient, featureConfig, aggregationConfig);
        }
        recreateSecondaryTable(athenaClient, featureConfig, aggregationConfig, day);
    }

    public void exportCsv(AthenaClient athenaClient, EventAggregatorAirlockConfig featureConfig, LocalDate day)
            throws AthenaException, AirlockException {
        String query = buildCsvExportQuery(featureConfig, day);
        executeQuery(athenaClient, featureConfig, query, true);
        query = buildDropCsvTableQuery(featureConfig, day);
        executeQuery(athenaClient, featureConfig, query, true);
    }

    public int calculateDailyValues(AthenaClient athenaClient, EventAggregatorAirlockConfig featureConfig, LocalDate day, long ts)
            throws AthenaException, AirlockException {
        String query = buildDailyCalcQuery(featureConfig, day, ts);
        executeQuery(athenaClient, featureConfig, query, true);
        int cnt = countDailyResults(athenaClient, featureConfig, day, ts);
        return cnt;
    }

    public int countDailyResults(AthenaClient athenaClient, EventAggregatorAirlockConfig featureConfig, LocalDate day, long ts)
            throws AthenaException, AirlockException {
        logger.info("Starting results count query for {}", day);
        String query = buildCountDailyQuery(featureConfig, day, ts);

        final AtomicInteger cnt = new AtomicInteger(0);
        executeQueryProcessResult(
                athenaClient,
                featureConfig,
                query,
                (rows, meta) -> {

                    if(rows.size() == 2) {//0 is header
                        Row row = rows.get(1);
                        List<Datum> cols = row.data();

                        if(!cols.isEmpty()) {
                            Datum col = cols.get(0);

                            try {
                                Integer v = Integer.valueOf(col.varCharValue());
                                cnt.set(v);
                            } catch (NumberFormatException e) {
                                logger.warn("Athena returned NaN: {}", col.varCharValue());
                            }
                        }
                    }

                });
        return cnt.get();
    }

    public void createDailyTable(
            AthenaClient athenaClient,
            EventAggregatorAirlockConfig featureConfig)
            throws AthenaException, AirlockException {
        String query = buildDailyTableCreationQuery(featureConfig);
        executeQuery(athenaClient, featureConfig, query, true);
    }

    public void dropTempDailyTable(AthenaClient athenaClient, EventAggregatorAirlockConfig featureConfig, LocalDate day, long ts)
            throws AthenaException, AirlockException {
        String query = buildDropTmpQuery(featureConfig, featureConfig.getDailyAggregationsTable(), day, ts);
        executeQuery(athenaClient, featureConfig, query, true);
    }

    public void calculateSecondaryValuesFirstRun(
            AthenaClient athenaClient,
            EventAggregatorAirlockConfig featureConfig,
            SecondaryAggregationConfig aggregationConfig,
            LocalDate day,
            long ts)
            throws AthenaException, AirlockException {
        String query = buildSecondaryCalcQueryFirstRun(featureConfig, aggregationConfig, day, ts);
        executeQuery(athenaClient, featureConfig, query, true);
    }

    public void dropTempSecondaryTable(AthenaClient athenaClient, EventAggregatorAirlockConfig featureConfig, SecondaryAggregationConfig aggregationConfig, LocalDate day, long ts)
            throws AthenaException, AirlockException {
        String query = buildDropTmpQuery(featureConfig, aggregationConfig.getTargetTable(), day, ts);
        executeQuery(athenaClient, featureConfig, query, true);
    }

    public Future<Exception> asyncCalculateSecondaryValues(
            AthenaClient athenaClient,
            EventAggregatorAirlockConfig featureConfig,
            SecondaryAggregationConfig aggregationConfig,
            LocalDate day,
            int partition) {
        return this.executorService.submit(() -> {

            try {
                long ts = System.currentTimeMillis();
                calculateSecondaryValuesForPartition(athenaClient, featureConfig, aggregationConfig, day, partition, ts);
                dropTempSecondaryTableForPartition(athenaClient, featureConfig, aggregationConfig, day, partition, ts);
                return null;
            } catch (Exception e) {
                logger.error("Aggregation error", e);
                return e;
            }
        });
    }

    public void calculateSecondaryValuesForPartition(
            AthenaClient athenaClient,
            EventAggregatorAirlockConfig featureConfig,
            SecondaryAggregationConfig aggregationConfig,
            LocalDate day,
            int partition,
            long ts)
            throws AthenaException, AirlockException {
        String query = buildSecondaryCalcQuery(featureConfig, aggregationConfig, day, partition, ts);
        executeQuery(athenaClient, featureConfig, query, (partition == 0));
    }

    public void dropTempSecondaryTableForPartition(
            AthenaClient athenaClient,
            EventAggregatorAirlockConfig featureConfig,
            SecondaryAggregationConfig aggregationConfig,
            LocalDate day,
            int partition,
            long ts)
            throws AthenaException, AirlockException {
        String query = buildDropTmpQuery(featureConfig, aggregationConfig.getTargetTable(), day, partition, ts);
        executeQuery(athenaClient, featureConfig, query, (partition == 99));
    }

    public void dropSecondaryTable(AthenaClient athenaClient, EventAggregatorAirlockConfig featureConfig, SecondaryAggregationConfig aggregationConfig)
            throws AthenaException, AirlockException {
        String query = buildDropSecondaryTableQuery(featureConfig, aggregationConfig);
        executeQuery(athenaClient, featureConfig, query, true);
    }

    public void recreateSecondaryTable(
            AthenaClient athenaClient,
            EventAggregatorAirlockConfig featureConfig,
            SecondaryAggregationConfig aggregationConfig,
            LocalDate day)
            throws AthenaException, AirlockException {
        String query = buildSecondaryTableCreationQuery(featureConfig, aggregationConfig, day);
        executeQuery(athenaClient, featureConfig, query, true);
    }

    public String getCsvFullPrefix(EventAggregatorAirlockConfig featureConfig, LocalDate day) {
        String outputFolder = S3Service.getS3UrlFolderPath(featureConfig.getCsvOutputFolder());
        String csvTableName = getCsvTableName(featureConfig.getCsvTablePrefix(), day);
        String csvPath = outputFolder + csvTableName;
        return csvPath;
    }

    private String buildDateCompletedQuery(EventAggregatorAirlockConfig featureConfig, LocalDate day) {
        StringBuilder sb = new StringBuilder();

        sb.append("select count(distinct partition) as cnt from ")
                .append(featureConfig.getSourceAthenaDb())
                .append('.')
                .append(featureConfig.getSourceEventsTable())
                .append(" where day = '")
                .append(day)
                .append("'");

        return sb.toString();
    }

    private String buildDailyCalcQuery(EventAggregatorAirlockConfig featureConfig, LocalDate day, long ts) {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE ")
                .append(featureConfig.getDestAthenaDb())
                .append('.')
                .append(getWholeTmpTableName(featureConfig.getDailyAggregationsTable(), day, ts))
                .append(" WITH (format = 'PARQUET',")
                .append(" external_location = '")
                .append("s3://")
                .append(featureConfig.getTargetS3Bucket())
                .append(S3Service.getS3UrlFolderPath(featureConfig.getDailyAggregationsS3Path()))
                .append("day=")
                .append(day)
                .append("',")
                .append(" partitioned_by = ARRAY['partition'],")
                .append(" bucketed_by = ARRAY['")
                .append(featureConfig.getTargetColumn())
                .append("'], ")
                .append(" bucket_count = 1) AS (select userid, shard, sum(cast(json_extract_scalar(event, '")
                .append(featureConfig.getAggregatedValueJsonPath())
                .append("') as ")
                .append(featureConfig.getAggregatedValueType())
                .append(")) as ")
                .append(featureConfig.getTargetColumn())
                .append(", count(1) as cnt,")
                .append(" partition from ")
                .append(featureConfig.getSourceAthenaDb())
                .append('.')
                .append(featureConfig.getSourceEventsTable())
                .append(" where day = '")
                .append(day)
                .append("' and name = '")
                .append(featureConfig.getEventName())
                .append("'")
                .append(" group by userid, shard, partition")
                .append(")");

        return sb.toString();
    }

    private String buildCountDailyQuery(EventAggregatorAirlockConfig featureConfig, LocalDate day, long ts) {
        StringBuilder sb = new StringBuilder();

        sb.append("select count(1) as cnt from ")
                .append(featureConfig.getDestAthenaDb())
                .append('.')
                .append(getWholeTmpTableName(featureConfig.getDailyAggregationsTable(), day, ts));

        return sb.toString();
    }

    private String buildDailyTableCreationQuery(EventAggregatorAirlockConfig featureConfig) {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE EXTERNAL TABLE `")
                .append(featureConfig.getDestAthenaDb())
                .append('.')
                .append(featureConfig.getDailyAggregationsTable())
                .append("`(`userid` string")
                .append(", `shard` bigint")
                .append(", `")
                .append(featureConfig.getTargetColumn())
                .append("` ")
                .append(featureConfig.getAggregatedValueType())
                .append(", `cnt` bigint")
                .append(")")
                .append(" PARTITIONED BY (`day` string, `partition` smallint)")
                .append(" ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'")
                .append(" STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'")
                .append(" OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'")
                .append(" LOCATION '")
                .append("s3://")
                .append(featureConfig.getTargetS3Bucket())
                .append(S3Service.getS3UrlFolderPath(featureConfig.getDailyAggregationsS3Path()))
                .append("'")
                .append(" TBLPROPERTIES (")
                .append("'has_encrypted_data'='false', ")
                .append("'projection.day.format'='yyyy-MM-dd', ")
                .append("'projection.day.interval.unit'='DAYS', ")
                .append("'projection.day.range'='2020-04-01, NOW', ")
                .append("'projection.day.type'='date',  ")
                .append("'projection.enabled'='true', ")
                .append("'projection.partition.range'='0,99', ")
                .append("'projection.partition.type'='integer'")
                .append(")");

        return sb.toString();
    }

    private String buildSecondaryCalcQueryFirstRun(
            EventAggregatorAirlockConfig featureConfig,
            SecondaryAggregationConfig aggregationConfig,
            LocalDate day,
            long ts) {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE ")
                .append(featureConfig.getDestAthenaDb())
                .append('.')
                .append(getWholeTmpTableName(aggregationConfig.getTargetTable(), day, ts))
                .append(" WITH (format = 'PARQUET',")
                .append(" external_location = '")
                .append("s3://")
                .append(featureConfig.getTargetS3Bucket())
                .append(S3Service.getS3UrlFolderPath(aggregationConfig.getTargetFolder()))
                .append("day=")
                .append(day)
                .append("',")
                .append(" partitioned_by = ARRAY['partition'],")
                .append(" bucketed_by = ARRAY['")
                .append(featureConfig.getTargetColumn())
                .append("'], ")
                .append(" bucket_count = 1) AS (SELECT userid, shard,")
                .append(featureConfig.getTargetColumn())
                .append(", cnt")
                .append(", partition")
                .append(" FROM ")
                .append(featureConfig.getDestAthenaDb())
                .append('.')
                .append(featureConfig.getDailyAggregationsTable())
                .append(" WHERE day = '")
                .append(day)
                .append("'")
                .append(")");

        return sb.toString();
    }

    private String buildSecondaryCalcQuery(
            EventAggregatorAirlockConfig featureConfig,
            SecondaryAggregationConfig aggregationConfig,
            LocalDate day,
            int partition,
            long ts) {
        StringBuilder sb = new StringBuilder();
        LocalDate oldStartDay = day.minusDays(aggregationConfig.getAggregationWindowDays());

        if(oldStartDay.isBefore(featureConfig.getHistoryStartDayDate())) {
            oldStartDay = featureConfig.getHistoryStartDayDate();
        }

        if(ChronoUnit.DAYS.between(oldStartDay, day.plusDays(1L)) <= aggregationConfig.getAggregationWindowDays()) {
            sb.append("CREATE TABLE ")
                    .append(featureConfig.getDestAthenaDb())
                    .append('.')
                    .append(getPartitionTmpTableName(aggregationConfig.getTargetTable(), day, partition, ts))
                    .append(" WITH (format = 'PARQUET',")
                    .append(" external_location = '")
                    .append("s3://")
                    .append(featureConfig.getTargetS3Bucket())
                    .append(S3Service.getS3UrlFolderPath(aggregationConfig.getTargetFolder()))
                    .append("day=")
                    .append(day)
                    .append("/partition=")
                    .append(partition)
                    .append("',")
                    .append(" bucketed_by = ARRAY['")
                    .append(featureConfig.getTargetColumn())
                    .append("'], ")
                    .append(" bucket_count = 1) AS (SELECT coalesce(c.userid, n.userid) as userid, coalesce(c.shard, n.shard) as shard,")
                    .append(" (coalesce(c.")
                    .append(featureConfig.getTargetColumn())
                    .append(", 0) + coalesce(n.")
                    .append(featureConfig.getTargetColumn())
                    .append(", 0)) as ")
                    .append(featureConfig.getTargetColumn())
                    .append(", (coalesce(c.cnt, 0) + coalesce(n.cnt, 0)) as cnt")
                    .append(", coalesce(c.partition, n.partition) as partition")
                    .append(" FROM (SELECT * FROM ")
                    .append(featureConfig.getDestAthenaDb())
                    .append('.')
                    .append(aggregationConfig.getTargetTable())
                    .append(" WHERE partition = ")
                    .append(partition)
                    .append(") c")
                    .append(" FULL JOIN (SELECT * FROM ")
                    .append(featureConfig.getDestAthenaDb())
                    .append('.')
                    .append(featureConfig.getDailyAggregationsTable())
                    .append(" WHERE day = '")
                    .append(day)
                    .append("' AND partition = ")
                    .append(partition)
                    .append(") n ON (c.userid = n.userid)")
                    .append(")");
        }
        else {
            sb.append("CREATE TABLE ")
                    .append(featureConfig.getDestAthenaDb())
                    .append('.')
                    .append(getPartitionTmpTableName(aggregationConfig.getTargetTable(), day, partition, ts))
                    .append(" WITH (format = 'PARQUET',")
                    .append(" external_location = '")
                    .append("s3://")
                    .append(featureConfig.getTargetS3Bucket())
                    .append(S3Service.getS3UrlFolderPath(aggregationConfig.getTargetFolder()))
                    .append("day=")
                    .append(day)
                    .append("/partition=")
                    .append(partition)
                    .append("',")
                    .append(" bucketed_by = ARRAY['")
                    .append(featureConfig.getTargetColumn())
                    .append("'], ")
                    .append(" bucket_count = 1) AS (SELECT coalesce(c.userid, n.userid) as userid, coalesce(c.shard, n.shard) as shard,")
                    .append(" (coalesce(c.")
                    .append(featureConfig.getTargetColumn())
                    .append(", 0) - coalesce(o.")
                    .append(featureConfig.getTargetColumn())
                    .append(", 0) + coalesce(n.")
                    .append(featureConfig.getTargetColumn())
                    .append(", 0)) as ")
                    .append(featureConfig.getTargetColumn())
                    .append(", (coalesce(c.cnt, 0) - coalesce(o.cnt, 0) + coalesce(n.cnt, 0)) as cnt")
                    .append(", coalesce(c.partition, n.partition) as partition")
                    .append(" FROM (SELECT * FROM ")
                    .append(featureConfig.getDestAthenaDb())
                    .append('.')
                    .append(aggregationConfig.getTargetTable())
                    .append(" WHERE partition = ")
                    .append(partition)
                    .append(") c")
                    .append(" LEFT JOIN (SELECT * FROM ")
                    .append(featureConfig.getDestAthenaDb())
                    .append('.')
                    .append(featureConfig.getDailyAggregationsTable())
                    .append(" WHERE day = '")
                    .append(oldStartDay)
                    .append("' AND partition = ")
                    .append(partition)
                    .append(") o ON (c.userid = o.userid)")
                    .append(" FULL JOIN (SELECT * FROM ")
                    .append(featureConfig.getDestAthenaDb())
                    .append('.')
                    .append(featureConfig.getDailyAggregationsTable())
                    .append(" WHERE day = '")
                    .append(day)
                    .append("' AND partition = ")
                    .append(partition)
                    .append(") n ON (c.userid = n.userid)")
                    .append(")");
        }
        return sb.toString();
    }

    private String buildSecondaryTableCreationQuery(EventAggregatorAirlockConfig featureConfig, SecondaryAggregationConfig aggregationConfig, LocalDate day) {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE EXTERNAL TABLE `")
                .append(featureConfig.getDestAthenaDb())
                .append('.')
                .append(aggregationConfig.getTargetTable())
                .append("`(`userid` string")
                .append(", `shard` bigint")
                .append(", `")
                .append(featureConfig.getTargetColumn())
                .append("` ")
                .append(featureConfig.getAggregatedValueType())
                .append(", `cnt` bigint")
                .append(")")
                .append(" PARTITIONED BY (`partition` smallint)")
                .append(" ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'")
                .append(" STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'")
                .append(" OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'")
                .append(" LOCATION '")
                .append("s3://")
                .append(featureConfig.getTargetS3Bucket())
                .append(S3Service.getS3UrlFolderPath(aggregationConfig.getTargetFolder()))
                .append("day=")
                .append(day)
                .append("'")
                .append(" TBLPROPERTIES (")
                .append("'has_encrypted_data'='false', ")
                .append("'projection.enabled'='true',")
                .append("'projection.partition.range'='0,99',")
                .append("'projection.partition.type'='integer'")
                .append(")");

        return sb.toString();
    }

    private String buildCsvExportQuery(EventAggregatorAirlockConfig featureConfig, LocalDate day) {

        if(StringUtils.isBlank(featureConfig.getCsvTablePrefix()) ||
            StringUtils.isBlank(featureConfig.getCsvOutputBucket()) ||
            CollectionUtils.isEmpty(featureConfig.getSecondaryAggregations())
        ) {
            throw new IllegalArgumentException("Invalid CSV output configuration");
        }

        String csvPath = getCsvFullPrefix(featureConfig, day);
        String csvOutput = "s3://" + featureConfig.getCsvOutputBucket() + csvPath + "/";
        String csvTableName = getCsvTableName(featureConfig.getCsvTablePrefix(), day);
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE ")
                .append(featureConfig.getDestAthenaDb())
                .append('.')
                .append(csvTableName)
                .append(" WITH (")
                .append(" format = 'TEXTFILE',")
                .append(" field_delimiter = ',',")
                .append(" external_location = '")
                .append(csvOutput)
                .append("',")
                .append(" bucketed_by = ARRAY['shard'], ")
                .append(" bucket_count = 1")
                .append(") AS (")
                .append("SELECT total.userid, total.shard")
                .append(", coalesce(d1.")
                .append(featureConfig.getTargetColumn())
                .append(", 0) as ")
                .append(featureConfig.getTargetColumn())
                .append("_d1")
                .append(", coalesce(d1.cnt, 0) as cnt_d1");

        SecondaryAggregationConfig total = null;

        for (SecondaryAggregationConfig aggregationConfig : featureConfig.getSecondaryAggregations()) {
            String joinedName = "d" + aggregationConfig.getAggregationWindowDays();

            if(aggregationConfig.getAggregationWindowDays() > 3650) {
                total = aggregationConfig;
                sb.append(", total.")
                        .append(featureConfig.getTargetColumn())
                        .append(" as ")
                        .append(featureConfig.getTargetColumn())
                        .append("_total")
                        .append(", total.cnt as cnt_total");
            } else {
                sb.append(", coalesce(")
                        .append(joinedName)
                        .append(".")
                        .append(featureConfig.getTargetColumn())
                        .append(", 0) as ")
                        .append(featureConfig.getTargetColumn())
                        .append("_")
                        .append(joinedName)
                        .append(", coalesce(")
                        .append(joinedName)
                        .append(".cnt, 0) as cnt_")
                        .append(joinedName);
            }
        }

        if(total == null) {
            throw new IllegalArgumentException("Invalid CSV output configuration");
        }
        sb.append(" FROM ")
            .append(featureConfig.getDestAthenaDb())
            .append('.')
            .append(total.getTargetTable())
            .append(" total")
            .append(" LEFT JOIN (SELECT * from ")
            .append(featureConfig.getDestAthenaDb())
            .append('.')
            .append(featureConfig.getDailyAggregationsTable())
            .append(" WHERE day = '")
            .append(day)
            .append("') d1 ON (total.userid = d1.userid)");

        for (SecondaryAggregationConfig aggregationConfig : featureConfig.getSecondaryAggregations()) {
            String joinedName = "d" + aggregationConfig.getAggregationWindowDays();

            if(aggregationConfig != total) {
                sb.append(" LEFT JOIN ")
                        .append(featureConfig.getDestAthenaDb())
                        .append('.')
                        .append(aggregationConfig.getTargetTable())
                        .append(" ")
                        .append(joinedName)
                        .append(" ON (total.userid = ")
                        .append(joinedName)
                        .append(".userid)");
            }
        }
        sb.append(")");

        return sb.toString();
    }

    private String buildDropSecondaryTableQuery(EventAggregatorAirlockConfig featureConfig, SecondaryAggregationConfig aggregationConfig) {
        StringBuilder sb = new StringBuilder();

        sb.append("DROP TABLE ")
                .append(featureConfig.getDestAthenaDb())
                .append('.')
                .append(aggregationConfig.getTargetTable());

        return sb.toString();
    }

    private String buildDropCsvTableQuery(EventAggregatorAirlockConfig featureConfig, LocalDate day) {
        StringBuilder sb = new StringBuilder();

        sb.append("DROP TABLE ")
                .append(featureConfig.getDestAthenaDb())
                .append('.')
                .append(getCsvTableName(featureConfig.getCsvTablePrefix(), day));

        return sb.toString();
    }

    private String getCsvTableName(String tablePrefix, LocalDate day) {
        return tablePrefix + "_" + day.toString().replaceAll("\\-", "");
    }
}
