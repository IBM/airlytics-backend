package com.ibm.weather.airlytics.common.athena;

import com.ibm.weather.airlytics.common.airlock.AirlockException;
import com.ibm.weather.airlytics.common.dto.AthenaAirlockConfig;
import com.ibm.weather.airlytics.common.dto.BasicAthenaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.ColumnInfo;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.Row;

import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

public abstract class AthenaBasicDao {

    private static final Logger logger = LoggerFactory.getLogger(AthenaBasicDao.class);

    protected static final int PARTITIONS_NUMBER = 100;

    protected AthenaDriver athena;

    //AWS standard limit for simultaneous DDL queries is 20, our limit is 60 (but there is still a very low rate limit)
    protected ExecutorService executorService = Executors.newFixedThreadPool(20);

    public AthenaBasicDao(AthenaDriver athena) {
        this.athena = athena;
    }

    protected void executeQuery(AthenaClient athenaClient, BasicAthenaConfig featureConfig, String query, boolean logging) throws AirlockException {
        if(logging) logger.info("Executing {}", query);
        String queryExecutionId = athena.submitAthenaQuery(query, athenaClient, featureConfig);
        if(logging) logger.info("Got Execution ID {}", queryExecutionId);
        QueryExecutionState result = athena.waitForQueryToComplete(queryExecutionId, athenaClient);
        if(logging) logger.info("Status for Execution ID {}: {}", queryExecutionId, result);

        switch (result) {
            case FAILED:
            case CANCELLED:
                throw new AirlockException("Athena query " + queryExecutionId + " " + result.toString() + ": " + query);
        }
    }

    protected void executeQueryProcessResult(
            AthenaClient athenaClient,
            BasicAthenaConfig featureConfig,
            String query,
            BiConsumer<List<Row>, List<ColumnInfo>> rowConsumer) throws AirlockException {
        logger.info("Executing {}", query);
        String queryExecutionId = athena.submitAthenaQuery(query, athenaClient, featureConfig);
        logger.info("Got Execution ID {}", queryExecutionId);
        QueryExecutionState result = athena.waitForQueryToComplete(queryExecutionId, athenaClient);
        logger.info("Status for Execution ID {}: {}", queryExecutionId, result);

        switch (result) {
            case FAILED:
            case CANCELLED:
                throw new AirlockException("Athena query " + queryExecutionId + " " + result.toString() + ": " + query);
        }
        athena.processResultRows(queryExecutionId, athenaClient, rowConsumer);
    }

    protected String getWholeTmpTableName(String targetTable, LocalDate day, long ts) {
        return "tmp_" + targetTable + "_" + day.toString().replaceAll("\\-", "") + "_" + String.valueOf(ts);
    }

    protected String getPartitionTmpTableName(String targetTable, LocalDate day, int partition, long ts) {
        return "tmp_" + targetTable + "_" + day.toString().replaceAll("\\-", "") + "_" + String.valueOf(ts) + "_" + partition;
    }

    protected String buildDropTmpQuery(AthenaAirlockConfig featureConfig, String targetTable, LocalDate day, long ts) {
        StringBuilder sb = new StringBuilder();

        sb.append("DROP TABLE ")
                .append(featureConfig.getDestAthenaDb())
                .append('.')
                .append(getWholeTmpTableName(targetTable, day, ts));

        return sb.toString();
    }

    protected String buildDropTmpQuery(AthenaAirlockConfig featureConfig, String targetTable, LocalDate day, int partition, long ts) {
        StringBuilder sb = new StringBuilder();

        sb.append("DROP TABLE ")
                .append(featureConfig.getDestAthenaDb())
                .append('.')
                .append(getPartitionTmpTableName(targetTable, day, partition, ts));

        return sb.toString();
    }
}
