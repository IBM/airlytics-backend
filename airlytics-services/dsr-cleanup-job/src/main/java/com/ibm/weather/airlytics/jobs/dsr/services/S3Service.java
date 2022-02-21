package com.ibm.weather.airlytics.jobs.dsr.services;

import com.ibm.weather.airlytics.common.airlock.AirlockException;
import com.ibm.weather.airlytics.jobs.dsr.dto.DeletedUserMarker;
import com.ibm.weather.airlytics.jobs.dsr.dto.DsrJobAirlockConfig;
import com.ibm.weather.airlytics.jobs.dsr.dto.NewDataFile;
import com.ibm.weather.airlytics.jobs.dsr.dto.PiTableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Consumer;

@Service
public class S3Service {

    private static final Logger logger = LoggerFactory.getLogger(S3Service.class);

    public static final String PATH_SEPARATOR = "/";

    public static String getS3UrlFolderPath(String configPath) {
        String result = configPath;

        if(!configPath.startsWith(PATH_SEPARATOR)) {
            result = PATH_SEPARATOR + result;
        }

        if(!result.endsWith(PATH_SEPARATOR)) {
            result = result + PATH_SEPARATOR;
        }
        return result;
    }

    public static String getS3KeyPrefix(String configPath) {
        String result = configPath;

        if(configPath.startsWith(PATH_SEPARATOR)) {
            result = result.substring(1);
        }

        if(!result.endsWith(PATH_SEPARATOR)) {
            result = result + PATH_SEPARATOR;
        }
        return result;
    }

    public void replaceShardPart(S3Client s3, DsrJobAirlockConfig featureConfig, NewDataFile shardPartPath) {
        processBucketObjects(s3, featureConfig.getTargetS3Bucket(), shardPartPath.getPath(), (key) -> {
                    int shard = DeletedUserMarker.extractShard(key);
                    LocalDate day = DeletedUserMarker.extractDay(key);
                    String objectName = key.substring(key.lastIndexOf(PATH_SEPARATOR) + 1);

                    String tableFolder =
                            getS3KeyPrefix(shardPartPath.getTableConfig().getSourceS3Folder()) +
                                    "shard=" + shard + PATH_SEPARATOR +
                                    "day=" + day.toString() + PATH_SEPARATOR;

                    processBucketObjects(s3, shardPartPath.getTableConfig().getSourceS3Bucket(), tableFolder, (deletedKey) -> {

                        if(featureConfig.isBackupOriginal()) {
                            renameObjectAddUnderscore(s3, shardPartPath.getTableConfig().getSourceS3Bucket(), deletedKey);
                        } else {
                            deleteObject(s3, shardPartPath.getTableConfig().getSourceS3Bucket(), deletedKey);
                        }
                    });
                    copyBucketObject(s3, featureConfig.getTargetS3Bucket(), key, shardPartPath.getTableConfig().getSourceS3Bucket(), tableFolder + objectName);
                });
    }

    public S3Client buildS3Client(DsrJobAirlockConfig featureConfig) throws AirlockException {
        return S3Client.builder().region(getRegion(featureConfig)).build();
    }

    public void processBucketObjects(S3Client s3, String bucketName, String folderPath, Consumer<String> processor) throws S3Exception {
        String prefix = folderPath;

        if(folderPath.startsWith(PATH_SEPARATOR)) {
            prefix = folderPath.substring(1);
        }
        ListObjectsV2Request request = ListObjectsV2Request
                .builder()
                .bucket(bucketName)
                .prefix(prefix)
                .build();
        ListObjectsV2Iterable response = s3.listObjectsV2Paginator(request);

        for (ListObjectsV2Response page : response) {
            page.contents().forEach((S3Object file) -> {
                processor.accept(file.key());
            });
        }
    }

    public void copyBucketObject(S3Client s3, String fromBucket, String objectKey, String toBucket, String newObjectKey) throws S3Exception {
        StringBuilder source = new StringBuilder();
        source.append(fromBucket);

        if(!objectKey.startsWith(PATH_SEPARATOR)) {
            source.append(PATH_SEPARATOR);
        }
        source.append(objectKey);
        String encodedUrl = null;

        try {
            encodedUrl = URLEncoder.encode(source.toString(), StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            // This should not happen
        }

        String destKey = newObjectKey;

        if(destKey.startsWith(PATH_SEPARATOR)) {
            destKey = newObjectKey.substring(1);
        }
        logger.info("Copying {} to {}/{}", source, toBucket, newObjectKey);
        CopyObjectRequest copyReq = CopyObjectRequest.builder()
                .copySource(encodedUrl)
                .destinationBucket(toBucket)
                .destinationKey(destKey)
                .build();

        s3.copyObject(copyReq);
    }

    public void deleteObject(S3Client s3, String bucketName, String objectKey) throws S3Exception {
        logger.info("Deleting {}/{}", bucketName, objectKey);
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build();

        s3.deleteObject(deleteObjectRequest);
    }

    public String renameObjectAddUnderscore(S3Client s3, String bucketName, String objectKey) throws S3Exception {
        String objectPrefix = objectKey.substring(0, objectKey.lastIndexOf(PATH_SEPARATOR) + 1);
        String objectName = objectKey.substring(objectKey.lastIndexOf(PATH_SEPARATOR) + 1);
        String newObjectKey = objectPrefix + "_" + objectName;
        copyBucketObject(s3, bucketName, objectKey, bucketName, newObjectKey);
        deleteObject(s3, bucketName, objectKey);
        return newObjectKey;
    }

    public void removeUnderscoredFiles(S3Client s3, String bucketName, String folderPath, int shard) {
        String prefix = folderPath + "shard=" + shard + PATH_SEPARATOR;
        processBucketObjects(s3, bucketName, prefix, (key) -> {
            String objectName = key.substring(key.lastIndexOf(PATH_SEPARATOR) + 1);

            if(objectName.startsWith("_")) {
                deleteObject(s3, bucketName, key);
            }
        });
    }

    private Region getRegion(DsrJobAirlockConfig featureConfig) throws AirlockException {

        if (featureConfig.getAthenaRegion().equalsIgnoreCase("eu-west-1")) {
            return Region.EU_WEST_1;
        }
        else if (featureConfig.getAthenaRegion().equalsIgnoreCase("us-east-1")) {
            return Region.US_EAST_1;
        }
        else {
            throw new AirlockException("unsupported athena region: " + featureConfig.getAthenaRegion() + ". Currently only eu-west-1 and us-east-1 are supported");
        }
    }
}
