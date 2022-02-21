package com.ibm.weather.airlytics.jobs.dsr.db;

import com.ibm.weather.airlytics.common.airlock.AirlockException;
import com.ibm.weather.airlytics.common.athena.AthenaBasicDao;
import com.ibm.weather.airlytics.common.athena.AthenaDriver;
import com.ibm.weather.airlytics.jobs.dsr.dto.DsrJobAirlockConfig;
import com.ibm.weather.airlytics.jobs.dsr.dto.NewDataFile;
import com.ibm.weather.airlytics.jobs.dsr.dto.PiTableConfig;
import com.ibm.weather.airlytics.jobs.dsr.services.S3Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;

import java.time.LocalDate;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;

@Repository
public class AthenaDao extends AthenaBasicDao {

    private static final Logger logger = LoggerFactory.getLogger(AthenaDao.class);

    @Autowired
    public AthenaDao(AthenaDriver athena) {
        super(athena);
    }

    public NewDataFile performJobChunk(
            AthenaClient athenaClient,
            DsrJobAirlockConfig featureConfig,
            PiTableConfig tableConfig,
            int shard,
            Set<String> userIds,
            LocalDate firstDay,
            LocalDate lastDay)
            throws AthenaException, AirlockException {
        logger.info("Starting cleanup in Athena for shard {} and period {}-{}", shard, firstDay, lastDay);
        String tempTableName = "temp_dsr_" + UUID.randomUUID().toString();

        String query = buildCreateTmpTableQuery(featureConfig, tableConfig, tempTableName, shard, userIds, firstDay, lastDay);
        executeQuery(athenaClient, featureConfig, query, true);

        String dropQuery = "DROP TABLE `" + tableConfig.getAthenaDb() + "`.`" + tempTableName + "`";
        executeQuery(athenaClient, featureConfig, dropQuery, true);
        String path = getTmpPath(featureConfig, tableConfig, tempTableName, shard);

        return new NewDataFile().path(path).tableConfig(tableConfig);
    }

    public Future<Exception> asyncJobChunk(
            AthenaClient athenaClient,
            DsrJobAirlockConfig featureConfig,
            PiTableConfig tableConfig,
            int shard,
            Set<String> userIds,
            LocalDate firstDay,
            LocalDate lastDay,
            Queue<NewDataFile> results) {

            return this.executorService.submit(() -> {

                try {
                    NewDataFile dataFile = performJobChunk(athenaClient, featureConfig, tableConfig, shard, userIds, firstDay, lastDay);
                    results.add(dataFile);
                    return null;
                } catch (Exception e) {
                    logger.error("DSR Cleanup error", e);
                    return e;
                }
            });
    }

    private String buildCreateTmpTableQuery(
            DsrJobAirlockConfig featureConfig,
            PiTableConfig tableConfig,
            String tempTableName,
            int shard,
            Set<String> userIds,
            LocalDate firstDay,
            LocalDate lastDay) {
        StringBuilder query = new StringBuilder(
                "CREATE TABLE \"" + tableConfig.getAthenaDb() + "\".\"" + tempTableName + "\"" +
                        " WITH (format = 'PARQUET'," +
                        " write_compression = 'SNAPPY'," +
                        " external_location = 's3://" + featureConfig.getTargetS3Bucket() + S3Service.getS3UrlFolderPath(getTmpPath(featureConfig, tableConfig, tempTableName, shard)) + "'," +
                        " partitioned_by = ARRAY['day']," +
                        " bucketed_by = ARRAY['" + tableConfig.getUserIdColumn() + "']," +
                        " bucket_count = 1) AS (" +
                        " select * from " + tableConfig.getAthenaDb() + "." + tableConfig.getPiTable() +
                        " where shard = " + shard +
                        " and day >= '" + firstDay + "'" +
                        " and day <= '" + lastDay + "'" +
                        " and " + tableConfig.getUserIdColumn() + " not in ("
        );

        StringBuilder ids = new StringBuilder();

        for(String userId : userIds) {
            ids.append("'").append(userId).append("',");
        }
        query.append(ids.substring(0, ids.length() - 1)).append("))");
        return query.toString();
    }

    private String getTmpPath(DsrJobAirlockConfig featureConfig, PiTableConfig tableConfig, String tempTableName, int shard) {
        String tempFolder = null;

        if(featureConfig.getTempFolder().endsWith("/")) {
            tempFolder = featureConfig.getTempFolder();
        } else {
            tempFolder = featureConfig.getTempFolder() + "/";
        }
        return tempFolder + tableConfig.getPiTable() + "/" + tempTableName + "/shard=" + shard;
    }
}
