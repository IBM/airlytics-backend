package com.ibm.weather.airlytics.jobs.dsr.services;

import com.ibm.weather.airlytics.common.airlock.AirlockException;
import com.ibm.weather.airlytics.common.athena.AthenaDriver;
import com.ibm.weather.airlytics.jobs.dsr.db.AthenaDao;
import com.ibm.weather.airlytics.jobs.dsr.dto.DeletedUserMarker;
import com.ibm.weather.airlytics.jobs.dsr.dto.DsrJobAirlockConfig;
import com.ibm.weather.airlytics.jobs.dsr.dto.NewDataFile;
import com.ibm.weather.airlytics.jobs.dsr.dto.PiTableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Service
public class JobExecutionService implements ApplicationContextAware {

    private static final Logger logger = LoggerFactory.getLogger(JobExecutionService.class);

    private static final int SHARDS_NUMBER = 1000;

    private ApplicationContext context;

    private DsrConfigService featureConfigService;

    private DeletionMarkerService deletionMarkerService;

    private S3Service s3Service;

    private AthenaDriver athena;

    private AthenaDao athenaDao;

    @Autowired
    public JobExecutionService(
            DsrConfigService featureConfigService,
            DeletionMarkerService deletionMarkerService,
            S3Service s3Service,
            AthenaDriver athena,
            AthenaDao athenaDao) {
        this.featureConfigService = featureConfigService;
        this.deletionMarkerService = deletionMarkerService;
        this.s3Service = s3Service;
        this.athena = athena;
        this.athenaDao = athenaDao;
    }

    @Override
    public void setApplicationContext(ApplicationContext ctx) throws BeansException {
        this.context = ctx;
    }

    @Scheduled(initialDelay = 10_000L, fixedDelay = Long.MAX_VALUE)
    public void runJob() {
        logger.info("Starting DSR Cleanup job");
        long startTs = System.currentTimeMillis();

        try {
            DsrJobAirlockConfig featureConfig = featureConfigService.getAirlockConfig();

            Queue<NewDataFile> paths = new ConcurrentLinkedQueue<>();// to store temp paths
            // Markers that may be saved by DSR Consumer after this Job loaded the current ones
            // will be processed in the next run of the job
            Map<Integer, List<DeletedUserMarker>> markersPerShard = new TreeMap<>();
            // produce the new DB dump files
            produceCleanDumps(featureConfig, paths, markersPerShard);
            // replace the old dump files with the new ones
            replaceDumps(featureConfig, paths, markersPerShard);

            logger.info("DSR Cleanup job finished. It took {}ms", (System.currentTimeMillis() - startTs));
            // shutdown
            int exitCode = SpringApplication.exit(this.context, () -> 0);
            System.exit(exitCode);
        } catch (Exception e) {
            logger.error("Error performing DSR Cleanup job", e);
            int exitCode = SpringApplication.exit(this.context, () -> 1);
            System.exit(exitCode);
            //throw new RuntimeException("Error performing DSR Cleanup job");
        }
    }

    private void produceCleanDumps(DsrJobAirlockConfig featureConfig, Queue<NewDataFile> paths, Map<Integer, List<DeletedUserMarker>> markersPerShard) throws AirlockException {
        AthenaClient athenaClient = athena.getAthenaClient(featureConfig);
        List<Future<Exception>> futures = new LinkedList<>();// to store running job parts

        // start cleaning each shard in parallel
        for(int shard = 0; shard < SHARDS_NUMBER; shard++) {
            List<DeletedUserMarker> shardMarkers = cleanupShard(featureConfig, athenaClient, futures, paths, shard);

            if(!shardMarkers.isEmpty()) {
                markersPerShard.put(shard, shardMarkers);
            }
        }

        // wait for all cleaned data to get produced
        for(Future<Exception> f : futures) {

            try {
                Exception inner = f.get();

                if(inner != null)  {
                    logger.warn("DSR Cleanup failed", inner);

                    if(inner instanceof  RuntimeException) {
                        throw (RuntimeException)inner;
                    } else if(inner instanceof AirlockException) {
                        throw (AirlockException)inner;
                    } else {
                        throw new AirlockException(inner);
                    }
                }
            } catch (ExecutionException | InterruptedException outer) {
                logger.warn("DSR Cleanup interrupted", outer);
                throw new AirlockException(outer);
            }
        }
    }

    private List<DeletedUserMarker> cleanupShard(
            DsrJobAirlockConfig featureConfig,
            AthenaClient athenaClient,
            List<Future<Exception>> futures,
            Queue<NewDataFile> paths,
            int shard) throws AirlockException {
        List<DeletedUserMarker> markers = deletionMarkerService.listCurrentMarkersForShard(featureConfig, shard);

        if(!markers.isEmpty()) {
            Set<String> userIds = markers.stream().map(DeletedUserMarker::getUserId).collect(Collectors.toSet());
            markers.sort(Comparator.comparing(DeletedUserMarker::getActivityDay));
            LocalDate firstDay = markers.get(0).getActivityDay();
            LocalDate lastDay = markers.get(markers.size() - 1).getActivityDay();
            long period = ChronoUnit.DAYS.between(firstDay, lastDay.plusDays(1L));

            if (period < 100) {
                featureConfig.getCleanupTables().forEach(tableConfig ->
                    futures.add(
                            athenaDao.asyncJobChunk(
                                    athenaClient,
                                    featureConfig,
                                    tableConfig,
                                    shard,
                                    userIds,
                                    firstDay,
                                    lastDay,
                                    paths)
                    )
                );
            } else {
                // split the job into chunks, no more than 100 days each
                LocalDate start = firstDay;
                LocalDate end = start.plusDays(99L);

                while (true) {

                    for(PiTableConfig tableConfig : featureConfig.getCleanupTables()) {
                        futures.add(
                                athenaDao.asyncJobChunk(
                                        athenaClient,
                                        featureConfig,
                                        tableConfig,
                                        shard,
                                        userIds,
                                        start,
                                        end,
                                        paths)
                        );
                    }

                    if (end.equals(lastDay)) {
                        break;
                    }
                    start = end.plusDays(1L);
                    end = start.plusDays(99L);

                    if (end.isAfter(lastDay)) {
                        end = lastDay;
                    }
                }
            }
        }
        return markers;
    }

    private void replaceDumps(DsrJobAirlockConfig featureConfig, Queue<NewDataFile> paths, Map<Integer, List<DeletedUserMarker>> markersPerShard) throws AirlockException {
        logger.info("Replacing {} files", paths.size());
        S3Client s3 = s3Service.buildS3Client(featureConfig);
        Map<Integer, List<NewDataFile>> pathsPerShard = new TreeMap<>(
                paths.stream().collect(Collectors.groupingBy(p -> DeletedUserMarker.extractShard(p.getPath())))
        );

        // For each shard, replace old data with the new cleaned up version,
        // and archive the processed deletion markers in EFS
        for(Integer shard : pathsPerShard.keySet()) {
            List<NewDataFile> shardParts = pathsPerShard.get(shard);
            Set<PiTableConfig> tableConfigs = new HashSet<>();
            logger.info("Replacing {} files in shard {}", shardParts.size(), shard);

            for(NewDataFile path : shardParts) {
                s3Service.replaceShardPart(s3, featureConfig, path);
                tableConfigs.add(path.getTableConfig());
            };

            if(featureConfig.isBackupOriginal()) {

                for (PiTableConfig tableConfig : tableConfigs) {
                    // remove underscored old files
                    s3Service.removeUnderscoredFiles(s3, tableConfig.getSourceS3Bucket(), tableConfig.getSourceS3Folder(), shard);
                }
            }
            deletionMarkerService.archiveProcessedMarkersForShard(featureConfig, markersPerShard.get(shard), shard);
        }
        logger.info("Replaced {} files in {} shards", paths.size(), pathsPerShard.size());
    }
}
