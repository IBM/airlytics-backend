package com.ibm.airlytics.retentiontrackerqueryhandler.db;


import com.amazonaws.services.logs.model.ExportTaskStatusCode;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import com.ibm.airlytics.retentiontracker.utils.TrackerTimer;
import com.ibm.airlytics.retentiontrackerqueryhandler.utils.ConfigurationManager;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.*;

import java.io.File;
import java.util.List;
import java.util.Objects;

@Component
@DependsOn({"ConfigurationManager", "Airlock"})
public class DbExporter {
    public static final long SLEEP_AMOUNT_IN_MS = 1000;

    private final ConfigurationManager configurationManager;
    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(DbExporter.class.getName());
    private final String iamRoleArn;
    private final String s3BucketName;
    private final String s3Prefix;
    private final String kmdKeyId;
    private final String sourceArn;
    private final String identifier;
    private final String dbInstanceIdentifier;
    private final String snapshotIdentifierPrefix;
    private final Region region;
    public DbExporter(ConfigurationManager configurationManager) {
        this.configurationManager = configurationManager;
        iamRoleArn = configurationManager.getDbExporterIamRole();
        s3BucketName = configurationManager.getDbExporterS3BucketName();
        kmdKeyId = configurationManager.getDbExporterKmsKeyId();
        s3Prefix = configurationManager.getDbExporterS3Prefix();
        sourceArn = configurationManager.getDbExporterSourceArn();
        identifier = configurationManager.getDbExporterIdentifier();
        dbInstanceIdentifier = configurationManager.getDbExporterDbInstanceIdentifier();
        snapshotIdentifierPrefix = configurationManager.getDbExporterSnapshotPrefix();

        String regionStr = configurationManager.getDbExporterRegion();
        this.region = Region.of(regionStr);
    }

    public String exportDb(String date, String snapshotArn) throws InterruptedException {
        TrackerTimer timer = new TrackerTimer();
        String suffix = "";
        RdsClient client = RdsClient.builder().region(this.region).build();
        StartExportTaskRequest exportTaskRequest = StartExportTaskRequest.builder().iamRoleArn(this.iamRoleArn)
                .s3BucketName(this.s3BucketName)
                .s3Prefix(s3Prefix)
                .kmsKeyId(this.kmdKeyId)
                .sourceArn(snapshotArn)
                .exportTaskIdentifier(identifier+date+suffix)
                .build();
        try {
            StartExportTaskResponse response = client.startExportTask(exportTaskRequest);
            String taskIdentifier = response.exportTaskIdentifier();
            waitForTaskToFinish(client, taskIdentifier);
            logger.info("taskIdentifier:"+exportTaskRequest);
            logger.info("finished export time in "+timer.getTotalTime()+" seconds");
        } catch (ExportTaskAlreadyExistsException e) {
            //if already exported, continue
        }
        return "s3://"+this.s3BucketName+ File.separator+s3Prefix+File.separator+identifier+date+suffix+File.separator;
    }

    private void waitForTaskToFinish(RdsClient client, String taskIdentifier) throws InterruptedException {
        DescribeExportTasksRequest request = DescribeExportTasksRequest.builder().exportTaskIdentifier(taskIdentifier).build();
        DescribeExportTasksResponse taskResponse;
        boolean isStillRunning = true;
        while (isStillRunning) {
            taskResponse = client.describeExportTasks(request);
            ExportTask exportTask = getExportTask(taskResponse.exportTasks(), taskIdentifier);
            String status = exportTask.status();
            if (status.equals(ExportTaskStatusCode.FAILED.toString())) {
                throw new RuntimeException("The Amazon Athena export failed to run with error message: " + exportTask.failureCause());
            } else if (status.equals(ExportTaskStatusCode.CANCELLED.toString())) {
                throw new RuntimeException("The Amazon Athena query was cancelled.");
            } else if (status.equals(ExportTaskStatusCode.COMPLETED.toString()) || status.equals("COMPLETE")) {
                isStillRunning = false;
            } else {
                // Sleep an amount of time before retrying again
                Thread.sleep(SLEEP_AMOUNT_IN_MS);
            }

        }
    }

    private ExportTask getExportTask(List<ExportTask> exportTasks, String taskIdentifier) {
        if (exportTasks==null) return null;
        for (ExportTask task : exportTasks) {
            if (Objects.equals(task.exportTaskIdentifier(),(taskIdentifier))) {
                return task;
            }
        }
        return null;
    }

    public String exportSnapshot(String day) throws InterruptedException {
        RdsClient client = RdsClient.builder().region(this.region).build();
        String snapshotArn = getSnapshotArn(client, day);
        if (snapshotArn==null) {
            logger.error("could not find snapshot for day:"+day);
            return null;
        }

        return exportDb(day, snapshotArn);

    }

    private String getSnapshotArn(RdsClient client, String day) {
        DescribeDbSnapshotsResponse resp = client.describeDBSnapshots();
        for (DBSnapshot dbSnapshot : resp.dbSnapshots()) {
            logger.info(dbSnapshot.dbSnapshotArn());
            if (dbSnapshot.dbInstanceIdentifier().equals(dbInstanceIdentifier) &&
                    dbSnapshot.dbSnapshotIdentifier().startsWith(snapshotIdentifierPrefix+day)) {
                //found our snapshot
                logger.info("found snapshot:"+dbSnapshot.dbSnapshotIdentifier());
                return dbSnapshot.dbSnapshotArn();
            }
        }
        return null;
    }
}
