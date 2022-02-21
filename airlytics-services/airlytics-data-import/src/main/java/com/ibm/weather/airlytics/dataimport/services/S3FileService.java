package com.ibm.weather.airlytics.dataimport.services;

import com.ibm.weather.airlytics.dataimport.db.UserFeaturesDao;
import com.ibm.weather.airlytics.dataimport.dto.DataImportAirlockConfig;
import com.ibm.weather.airlytics.dataimport.dto.DataImportConfig;
import com.ibm.weather.airlytics.dataimport.dto.ImportJobDefinition;
import com.ibm.weather.airlytics.dataimport.dto.S3UriData;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPInputStream;

@Service
public class S3FileService {
    private static final Logger logger = LoggerFactory.getLogger(S3FileService.class);

    private static final String TMP_FOLDER = "./tmp";

    private UserFeaturesDao userFeaturesDao;

    private String prodDbSchema;

    @Autowired
    public S3FileService(
            UserFeaturesDao userFeaturesDao,
            @Value("${spring.datasource.jdbc-url}") String dbUrl) {
        this.userFeaturesDao = userFeaturesDao;

        try {
            Files.createDirectories(Paths.get(TMP_FOLDER));
        } catch (IOException e) {
            logger.error("Could not create temp directory " + TMP_FOLDER, e);
            throw new RuntimeException(e);
        }
        this.prodDbSchema = dbUrl.substring(dbUrl.lastIndexOf('=') + 1);
    }

    public ImportJobDefinition buildJobDefinition(DataImportConfig jobRequest, DataImportAirlockConfig config)
            throws DataImportServiceException, S3Exception {

        if(!config.isAllowedTable(jobRequest.getTargetTable())) {
            throw new DataImportServiceException("Table "+ jobRequest.getTargetTable() + " is not allowed for data import");
        }

        try {
            ImportJobDefinition job = new ImportJobDefinition(jobRequest, prodDbSchema, config);

            processFileData(jobRequest, job);
            prepareFileForImport(job, config);

            String header = readFirstLine(job, config);// also fixes GZIP meta

            if(!job.isWithHeader()) {
                // this is an aggregation import
                StringBuilder sb = new StringBuilder();
                sb.append(UserFeaturesDao.USER_ID_COLUMN);

                for(String column : jobRequest.getAffectedColumns()) {
                    sb.append(',').append(column);
                }
                header = sb.toString();
            }
            processCsvHeader(header, job);

            return job;
        } catch (DataImportServiceException dise) {
            throw dise;
        } catch (S3Exception s3e) {
            throw s3e;
        } catch (Exception e) {
            throw new DataImportServiceException("Error reading data from S3", e);
        }
    }

    protected void processFileData(DataImportConfig jobRequest, ImportJobDefinition job) {
        URI uri = URI.create(preprocessUrlStr(jobRequest.getS3File(), true));
        String bucket, filePath;

        if ("s3".equalsIgnoreCase(uri.getScheme())) {

            bucket = uri.getAuthority();

            if (bucket == null) {
                throw new IllegalArgumentException("Invalid S3 URI: no bucket: " + uri);
            }

            String path = uri.getPath();
            if (path.length() <= 1) {
                // s3://bucket or s3://bucket/
                throw new IllegalArgumentException("Invalid S3 URI: no file path: " + uri);
            } else {
                // s3://bucket/key
                // Remove the leading '/'.
                filePath = uri.getPath().substring(1);
            }
        } else {
            throw new IllegalArgumentException("Invalid S3 URI: " + uri);
        }
        // job definition - set bucket and path
        job.getS3Uri().setS3Bucket(bucket);
        job.getS3Uri().setS3FilePath(filePath);
        logger.info("Creating a job for {}", job.getS3Uri().toString());
    }

    protected void prepareFileForImport(ImportJobDefinition job, DataImportAirlockConfig config)
            throws DataImportServiceException, S3Exception {

        if(StringUtils.isNotBlank(config.getTempBucketFolderPath())) {
            String destBucket = config.getTempBucketFolderPath();
            String destFolderPath = "";
            int slashIdx = destBucket.indexOf('/');

            if(slashIdx > 0) {
                destBucket = config.getTempBucketFolderPath().substring(0, slashIdx);
                destFolderPath = config.getTempBucketFolderPath().substring(slashIdx + 1);
            }
            S3UriData destFileUri = new S3UriData(config.getDbAwsRegion(), destBucket, destFolderPath);

            String targetLocation = destFileUri.getS3Region();

            if(StringUtils.isBlank(targetLocation)) {
                targetLocation = fetchBucketLocation(destFileUri, config);
                destFileUri.setS3Region(targetLocation);
            }
            S3UriData sourceFileUri = job.getS3Uri();
            String sourceLocation = sourceFileUri.getS3Region();

            if(StringUtils.isBlank(sourceLocation)) {
                sourceLocation = fetchBucketLocation(sourceFileUri, config);
                sourceFileUri.setS3Region(sourceLocation);
            }

            if(config.isAlwaysCopyToTemp() || !targetLocation.equalsIgnoreCase(sourceLocation)) {
                S3Client sourceS3 = buildS3Client(sourceFileUri, config);
                HeadObjectResponse head = sourceS3.headObject(builder -> builder.bucket(sourceFileUri.getS3Bucket()).key(sourceFileUri.getS3FilePath()));

                S3Client targetS3 = buildS3Client(destFileUri, config);

                String targetFileName = sourceFileUri.getS3FilePath().substring(sourceFileUri.getS3FilePath().lastIndexOf('/') + 1);
                String targetPath = (StringUtils.isNotBlank(destFileUri.getS3FilePath())) ? (destFileUri.getS3FilePath() + '/' + targetFileName) : targetFileName;
                destFileUri.setS3FilePath(targetPath);

                if(config.isAlwaysGetPut()) {
                    getPutObject(sourceS3, sourceFileUri, head, targetS3, destFileUri, targetFileName);
                } else {
                    copyObject(sourceS3, sourceFileUri, head, targetS3, destFileUri, targetFileName);
                }
                job.setS3Uri(destFileUri);
            }
        }
    }

    private void copyObject(
            S3Client sourceS3,
            S3UriData sourceFileUri,
            HeadObjectResponse head,
            S3Client targetS3,
            S3UriData destFileUri,
            String targetFileName) throws DataImportServiceException, S3Exception {

        try {

            if (isGzip(sourceFileUri, head)) {
                targetS3.copyObject(builder -> builder
                        .copySource(sourceFileUri.getS3Bucket() + '/' + sourceFileUri.getS3FilePath())
                        .destinationBucket(destFileUri.getS3Bucket())
                        .destinationKey(destFileUri.getS3FilePath())
                        .contentType(head.contentType())
                        .metadata(head.metadata())
                        .metadataDirective("REPLACE")
                        .contentEncoding("gzip"));
            } else {
                targetS3.copyObject(builder -> builder
                        .copySource(sourceFileUri.getS3Bucket() + '/' + sourceFileUri.getS3FilePath())
                        .destinationBucket(destFileUri.getS3Bucket())
                        .destinationKey(destFileUri.getS3FilePath())
                        .contentType(head.contentType())
                        .metadata(head.metadata())
                        .metadataDirective("REPLACE"));
            }
        } catch (S3Exception s3e) {

            if (s3e.statusCode() == 403) {

                try {
                    getPutObject(sourceS3, sourceFileUri, head, targetS3, destFileUri, targetFileName);
                } catch (Exception e1) {
                    throw e1;
                }
            } else {
                logger.warn("Could not copy from " + sourceFileUri.toString() + " to " + destFileUri.toString(), s3e);
                throw s3e;
            }
        } catch (Exception e) {
            throw new DataImportServiceException("Error copying object to a temporary S3 bucket", e);
        }
    }

    private void getPutObject(
            S3Client sourceS3,
            S3UriData sourceFileUri,
            HeadObjectResponse head,
            S3Client targetS3,
            S3UriData destFileUri,
            String targetFileName) throws DataImportServiceException, S3Exception {
        File tempFile = new File(TMP_FOLDER + "/" + targetFileName);

        try {

            if(tempFile.exists()) {
                tempFile.delete();
            }
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(sourceFileUri.getS3Bucket())
                    .key(sourceFileUri.getS3FilePath())
                    .build();
            ResponseInputStream inputStream = sourceS3.getObject(getObjectRequest);
            FileUtils.copyInputStreamToFile(inputStream, tempFile);            

            PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder()
                    .bucket(destFileUri.getS3Bucket())
                    .key(destFileUri.getS3FilePath())
                    .contentType(head.contentType())
                    .metadata(head.metadata());

            if (isGzip(sourceFileUri, head)) {
                putObjectRequestBuilder = putObjectRequestBuilder.contentEncoding("gzip");
            }
            PutObjectRequest putObjectRequest = putObjectRequestBuilder.build();
            targetS3.putObject(putObjectRequest, RequestBody.fromFile(tempFile));
        } catch (S3Exception s3e) {
            logger.warn("Could not copy from " + sourceFileUri.toString() + " to " + destFileUri.toString(), s3e);
            throw s3e;
        } catch (Exception e) {
            logger.error("Could not get object from " + sourceFileUri.toString() + " and put to " + destFileUri.toString(), e);
            throw new DataImportServiceException("Error copying object to a temporary S3 bucket", e);
        } finally {

            if(tempFile.exists()) {
                tempFile.delete();
            }
        }
    }

    protected void processCsvHeader(String header, ImportJobDefinition job) throws DataImportServiceException {
        String[] columns = parseCsvHeader(header);

        if(columns == null || columns.length < 2) {
            throw new DataImportServiceException("Invalid CSV header for " + job.getTargetSchema() + "." + job.getTargetTable() + ": " + header);
        }

        if(!UserFeaturesDao.USER_ID_COLUMN.equalsIgnoreCase(columns[0])) {
            throw new DataImportServiceException("Invalid CSV header, the 1st column expected: " + UserFeaturesDao.USER_ID_COLUMN + ", but was: " + columns[0]);
        }

        Map<String, String> allColumns = userFeaturesDao.getTableColumns(job.getTargetSchema(), job.getTargetTable());

        List<String> lst = Arrays.asList(columns);
        List<String> unknown = new LinkedList<>();
        Map<String, String> present = new LinkedHashMap<>();

        lst.forEach(column -> {

            if(!allColumns.containsKey(column.toLowerCase())) {
                unknown.add(column);
            } else {
                present.put(column.toLowerCase(), allColumns.get(column.toLowerCase()));
            }
        });

        if(!unknown.isEmpty()) {
            throw new DataImportServiceException("Invalid CSV header for " + job.getTargetTable() + ": unexisting columns: " + unknown);
        }
        // job definition - set columns
        job.setColumns(present);
    }

    private S3Client buildS3Client(S3UriData s3Uri, DataImportAirlockConfig config) throws DataImportServiceException {

        try {
            String location = s3Uri.getS3Region();

            if(StringUtils.isBlank(location)) {
                location = fetchBucketLocation(s3Uri, config);
                s3Uri.setS3Region(location);
            }
            Region region = Region.of(location);

            // job definition - set region
            return S3Client.builder().region(region).build();
        } catch (S3Exception s3e) {
            throw new DataImportServiceException("Error connecting to S3 URI " + s3Uri.toString(), s3e);
        }
    }

    private String fetchBucketLocation(S3UriData s3Uri, DataImportAirlockConfig config) {
        Region region = StringUtils.isNotBlank(config.getDbAwsRegion()) ? Region.of(config.getDbAwsRegion()) : Region.EU_WEST_1;
        S3Client s3 = S3Client.builder().region(region).build();
        String location = null;

        try {
            HeadBucketResponse resp = s3.headBucket(builder -> builder.bucket(s3Uri.getS3Bucket()));
            List<String> headerValues = resp.sdkHttpResponse().headers().get("x-amz-bucket-region");

            if(CollectionUtils.isNotEmpty(headerValues)) {
                location = headerValues.get(0);
            }
        } catch (S3Exception e) {
            if (e.statusCode() == 301) {
                List<String> errorHeaderValues = e.awsErrorDetails().sdkHttpResponse().headers().get("x-amz-bucket-region");

                if(CollectionUtils.isNotEmpty(errorHeaderValues)) {
                    location = errorHeaderValues.get(0);
                } else {
                    logger.warn("Getting x-amz-bucket-region from response failed for bucket " + s3Uri.toString(), e);
                }
            } else {
                logger.warn("Head request failed for bucket " + s3Uri.toString(), e);
            }
        } catch (Exception e) {
            logger.warn("Head request failed for bucket " + s3Uri.toString(), e);
        }

        if (StringUtils.isBlank(location)) {
            try {
                location = s3.getBucketLocation(builder -> builder.bucket(s3Uri.getS3Bucket())).locationConstraintAsString();
            } catch (Exception e) {
                logger.warn("Bucket Location request failed for bucket " + s3Uri.toString(), e);
            }
        }

        if (StringUtils.isBlank(location) || "none".equalsIgnoreCase(location)) {
            logger.warn("Setting default region us-east-1");
            location = "us-east-1";// default region
        }
        return location;
    }

    private String readFirstLine(ImportJobDefinition job, DataImportAirlockConfig config) throws DataImportServiceException {
        S3UriData s3Uri = job.getS3Uri();
        S3Client s3 = buildS3Client(s3Uri, config);
        HeadObjectResponse head = s3.headObject(builder -> builder.bucket(s3Uri.getS3Bucket()).key(s3Uri.getS3FilePath()));

        ResponseInputStream<GetObjectResponse> s3objectResponse =
                s3.getObject(builder -> builder
                        .bucket(s3Uri.getS3Bucket())
                        .key(s3Uri.getS3FilePath())
                        .range("bytes=0-19999"));

        if(isGzip(s3Uri, head)) {

            try (InputStream fileStream = new BufferedInputStream(s3objectResponse);
                 GZIPInputStream gzipStream = new GZIPInputStream(fileStream);
                 InputStreamReader decoder = new InputStreamReader(gzipStream, "UTF-8")) {
                char[] buf = new char[20000];
                decoder.read(buf, 0, 20000);
                String line = new String(buf);
                line = line.substring(0, line.indexOf('\n'));

                if(!"gzip".equalsIgnoreCase(head.contentEncoding())) {

                    for(int i = 0; i < 2; i++) {

                        try {
                            s3.copyObject(builder -> builder
                                    .copySource(s3Uri.getS3Bucket() + '/' + s3Uri.getS3FilePath())
                                    .destinationBucket(s3Uri.getS3Bucket())
                                    .destinationKey(s3Uri.getS3FilePath())
                                    .contentType(head.contentType())
                                    .metadata(head.metadata())
                                    .metadataDirective("REPLACE")
                                    .contentEncoding("gzip")
                                    .acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL));
                            break;
                        } catch(SdkException e) {

                            if(i == 1) throw e;
                            logger.warn("AWS Exception {}. Retrying...", e.getMessage());
                        }
                    }
                }
                return line;
            } catch (IOException e) {
                throw new DataImportServiceException("Error reading CSV header from S3", e);
            }
        } else {

            try(BufferedReader reader = new BufferedReader(new InputStreamReader(s3objectResponse))) {
                return reader.readLine();
            } catch (IOException e) {
                throw new DataImportServiceException("Error reading CSV header from S3", e);
            }
        }
    }

    private boolean isGzip(S3UriData sourceFileUri, HeadObjectResponse head) {
        return sourceFileUri.getS3FilePath().endsWith(".gz") ||
                sourceFileUri.getS3FilePath().endsWith(".gzip") ||
                "application/x-gzip".equalsIgnoreCase(head.contentType()) ||
                (head != null && "gzip".equalsIgnoreCase(head.contentEncoding()));
    }

    private String[] parseCsvHeader(String header) throws DataImportServiceException {
        CSVParser parser = new CSVParserBuilder()
                .withSeparator(',')
                .build();

        try (CSVReader csvReader = new CSVReaderBuilder(new StringReader(header))
                .withSkipLines(0)
                .withCSVParser(parser)
                .build();) {
            List<String[]> lines = csvReader.readAll();
            return lines.get(0);
        }  catch (IOException | CsvException e) {
            throw new DataImportServiceException("Error parsing CSV header from S3", e);
        }
    }

    private String preprocessUrlStr(final String str, final boolean encode) {
        if (encode) {
            try {
                return (URLEncoder.encode(str, "UTF-8")
                        .replace("%3A", ":")
                        .replace("%2F", "/")
                        .replace("+", "%20"));
            } catch (UnsupportedEncodingException e) {
                // This should never happen unless there is something
                // fundamentally broken with the running JVM.
                throw new RuntimeException(e);
            }
        }
        return str;
    }
}
