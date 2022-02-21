package com.ibm.weather.airlytics.amplitude.service;

import com.ibm.weather.airlytics.amplitude.db.AmplitudeExportDao;
import com.opencsv.exceptions.CsvException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

@Service
public class S3FileService {
    private static final Logger logger = LoggerFactory.getLogger(S3FileService.class);

    private static final String TMP_FOLDER = "./tmp";

    private AtomicInteger usersCount = new AtomicInteger(0);

    @Value("${export.users.platform:ios}")
    private String platform;

    @Value("${amplitude.batch.size:100}")
    private int amplitudeBatchSize;

    @Value("${country.enabled:false}")
    private boolean countryEnabled;

    @Value("${country.userid.obfuscate:false}")
    private boolean countryUserIdObfuscate;

    @Value("${s3.region:}")
    private String s3Region;

    @Value("${s3.bucket:}")
    private String s3Bucket;

    @Value("${s3.path:}")
    private String s3Path;

    @Value("${shards.pause.ms:90000}")
    private long pauseMs;

    @Value("${shards.pause.after:20}")
    private int shardsToPause;

    private AmplitudeExportDao dao;

    private AmplitudeHistoryUploadService amplitudeService;

    @Autowired
    public S3FileService(AmplitudeExportDao dao, AmplitudeHistoryUploadService amplitudeService) {
        this.dao = dao;
        this.amplitudeService = amplitudeService;
    }

    @PostConstruct
    public void init() {

        try {
            Files.createDirectories(Paths.get(TMP_FOLDER));
        } catch (IOException e) {
            logger.error("Could not create temp directory " + TMP_FOLDER, e);
            throw new RuntimeException(e);
        }
    }

    public int getProcessedCount() {
        return this.usersCount.get();
    }

    public void sendCountyLanguage() {

        if(this.amplitudeService.isError()) return;

        if(!countryEnabled) return;

        if(StringUtils.isAnyBlank(s3Region, s3Bucket, s3Path)) return;

        long ts = System.currentTimeMillis();
        File idsFile = getS3Object(TMP_FOLDER + "/" + UUID.randomUUID());
        logger.info("Downloaded {} to {} in {}ms", s3Bucket + "/" + s3Path, idsFile.getAbsolutePath(), (System.currentTimeMillis() - ts));
        processUserIdsFile(idsFile);
    }

    private S3Client buildS3Client() {

        try {
            Region region = Region.of(s3Region);
            return S3Client.builder().region(Region.of(s3Region)).build();
        } catch (S3Exception s3e) {
            throw new RuntimeException("Error connecting to S3 URI " + s3Bucket + "/" + s3Path, s3e);
        }
    }

    private File getS3Object(String targetFileName) {
        File tempFile = new File(TMP_FOLDER + "/" + targetFileName);

        try {
            S3Client s3 = buildS3Client();

            if(tempFile.exists()) {
                tempFile.delete();
            }
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(s3Bucket)
                    .key(s3Path)
                    .build();
            ResponseInputStream inputStream = s3.getObject(getObjectRequest);
            FileUtils.copyInputStreamToFile(inputStream, tempFile);

            return tempFile;
        } catch (S3Exception s3e) {
            logger.warn("Could not copy from " + s3Bucket + "/" + s3Path + " to local folder", s3e);
            throw s3e;
        } catch (Exception e) {
            logger.error("Could not get object from " + s3Bucket + "/" + s3Path, e);
            throw new RuntimeException("Error copying object to a temporary S3 bucket", e);
        }
    }

    private void processUserIdsFile(File f) {
        BatchingConsumer consumer = new BatchingConsumer(this.amplitudeService, this.usersCount, this.amplitudeBatchSize);
        List<String> batch = new ArrayList<>(1000);
        int batchNumber = 0;

        try(FileInputStream inputStream = new FileInputStream(f);
            Scanner sc = new Scanner(inputStream, "UTF-8");) {
            sc.nextLine();//skip first

            while (sc.hasNextLine()) {
                String userId = sc.nextLine();

                if(countryUserIdObfuscate) {
                    userId = obfuscateValue(userId);
                }

                if(StringUtils.isNotBlank(userId)) {
                    batch.add(userId);

                    if (batch.size() >= amplitudeBatchSize) {
                        long ts = System.currentTimeMillis();
                        dao.sendCountryLanguage(platform, batch, consumer);
                        logger.warn("Batch select took {}ms", System.currentTimeMillis() - ts);
                        waitIfNeeded(++batchNumber);
                        batch = new ArrayList<>(1000);
                    }
                }
            }

            if(!batch.isEmpty()) {
                dao.sendCountryLanguage(platform, batch, consumer);
            }
            consumer.sendBatch();
            // note that Scanner suppresses exceptions
            if (sc.ioException() != null) {
                throw sc.ioException();
            }
        }   catch (IOException e) {
            throw new RuntimeException("Error parsing CSV file from S3", e);
        }
    }

    private void waitIfNeeded(int batchNumber) {

        if (shardsToPause > 0 && pauseMs > 0L && (batchNumber % shardsToPause) == 0) {
            logger.info("Giving DB a break. Sleep for {}ms", pauseMs);

            try {
                Thread.sleep(pauseMs);
            } catch (InterruptedException e) {
            }
        }
    }

    private String obfuscateValue(String value) {

        if (StringUtils.isNotBlank(value)) {
            String prefix = "aaa";
            String obf = UUID.nameUUIDFromBytes(value.getBytes()).toString();
            if (StringUtils.isNotBlank(prefix)) {// replace bytes at the start
                obf = prefix + obf.substring(prefix.length());
            }
            return obf;
        }
        return "";
    }
}
