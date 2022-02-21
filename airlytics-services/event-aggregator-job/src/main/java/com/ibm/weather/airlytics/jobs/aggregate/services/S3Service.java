package com.ibm.weather.airlytics.jobs.aggregate.services;

import com.ibm.weather.airlytics.common.airlock.AirlockException;
import com.ibm.weather.airlytics.jobs.aggregate.dto.EventAggregatorAirlockConfig;
import com.ibm.weather.airlytics.jobs.aggregate.dto.SecondaryAggregationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.time.LocalDate;
import java.util.*;
import java.util.function.Consumer;

@Service
public class S3Service {

    private static final Logger logger = LoggerFactory.getLogger(S3Service.class);

    public static final String PATH_SEPARATOR = "/";
    private static final String SUCCESS_PREFIX = "_SUCCESS_";

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

    public void markSuccess(EventAggregatorAirlockConfig featureConfig, LocalDate day)
            throws S3Exception, AirlockException {
        S3Client s3 = buildS3Client(featureConfig);
        String path = getS3KeyPrefix(featureConfig.getDailyAggregationsS3Path()) + SUCCESS_PREFIX + day.toString();

        PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(featureConfig.getTargetS3Bucket())
                .key(path)
                .build();
        PutObjectResponse resp = s3.putObject(putRequest, RequestBody.empty());
        logger.info("Creating {}/{} returned {}", featureConfig.getTargetS3Bucket(), path, resp.sdkHttpResponse().statusCode());
    }

    public SortedSet<LocalDate> getMissingDays(EventAggregatorAirlockConfig featureConfig)
            throws S3Exception, AirlockException {
        LocalDate next = featureConfig.getHistoryStartDayDate();
        LocalDate today = LocalDate.now();
        final SortedSet<LocalDate> days = new TreeSet<>();

        while(next.isBefore(today)) {
            days.add(next);
            next = next.plusDays(1L);
        }

        S3Client s3 = buildS3Client(featureConfig);
        processBucketObjects(
                s3,
                featureConfig.getTargetS3Bucket(),
                featureConfig.getDailyAggregationsS3Path() + SUCCESS_PREFIX,
                path -> {
                    LocalDate found = extractDateFromPath(path);

                    if(found != null) {
                        days.remove(found);
                    }
                });

        return days;
    }

    public void missingDaysCleanup(EventAggregatorAirlockConfig featureConfig, Set<LocalDate> missingDays)
            throws S3Exception, AirlockException {
        List<String> folders = new ArrayList<>(featureConfig.getSecondaryAggregations().size() + 1);
        folders.add(featureConfig.getDailyAggregationsS3Path());
        featureConfig.getSecondaryAggregations().forEach(sa -> folders.add(sa.getTargetFolder()));

        S3Client s3 = buildS3Client(featureConfig);

        folders.forEach(folder -> {

            missingDays.forEach(day -> {
                processBucketObjects(
                        s3,
                        featureConfig.getTargetS3Bucket(),
                        folder + "day=" + day.toString() + PATH_SEPARATOR,
                        path -> {
                            deleteObject(s3, featureConfig.getTargetS3Bucket(), path);
                        });
            });
        });
    }

    public String getCsvS3Url(EventAggregatorAirlockConfig featureConfig, String folderPath)
            throws S3Exception, AirlockException {
        S3Client s3 = buildS3Client(featureConfig);

        String prefix = folderPath;

        if(folderPath.startsWith("/")) {
            prefix = folderPath.substring(1);
        }
        ListObjectsV2Request request = ListObjectsV2Request
                .builder()
                .bucket(featureConfig.getCsvOutputBucket())
                .prefix(prefix)
                .build();
        ListObjectsV2Iterable response = s3.listObjectsV2Paginator(request);

        for (ListObjectsV2Response page : response) {
            List<S3Object> lst = page.contents();

            for(S3Object s3o : lst) {

                if(s3o.key().endsWith(".gz")) {
                    return "s3://" + featureConfig.getCsvOutputBucket() + "/" + s3o.key();
                }
            }
        }
        return null;
    }

    private S3Client buildS3Client(EventAggregatorAirlockConfig featureConfig) throws AirlockException {
        return S3Client.builder().region(getRegion(featureConfig)).build();
    }

    private Region getRegion(EventAggregatorAirlockConfig featureConfig) throws AirlockException {

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

    private void processBucketObjects(S3Client s3, String bucketName, String filePrefix, Consumer<String> processor) throws S3Exception {
        String prefix = filePrefix;

        if(filePrefix.startsWith("/")) {
            prefix = filePrefix.substring(1);
        }
        logger.info("Searching for {}*", prefix);
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

    private void deleteObject(S3Client s3, String bucketName, String objectKey) throws S3Exception {
        logger.info("Deleting {}/{}", bucketName, objectKey);
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build();

        s3.deleteObject(deleteObjectRequest);
    }

    private LocalDate extractDateFromPath(String path) {
        int daySectionStart = path.indexOf(SUCCESS_PREFIX);

        if(daySectionStart >= 0) {
            String date = path.substring(daySectionStart + SUCCESS_PREFIX.length(), daySectionStart  + SUCCESS_PREFIX.length() + 10);
            return LocalDate.parse(date);
        }
        return null;
    }
}
