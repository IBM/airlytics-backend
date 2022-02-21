package com.ibm.airlytics.retentiontrackerqueryhandler.db.data;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;

public class S3Serializer implements DataSerializer {
    private static final Logger LOGGER = Logger.getLogger(S3Serializer.class.getName());

    public static final int RETRY_INTERVAL_MS = 1000;
    public static final int S3_MAX_DELETED_FILES = 1000; //S3 can delete up to 1000 files at once

    private AmazonS3 s3client;
    private String bucketName;
    private int ioActionRetries;

    public S3Serializer(String s3region, String bucketName, int ioActionRetries) {
        this.bucketName = bucketName;
        this.ioActionRetries = ioActionRetries;
        s3client = AmazonS3ClientBuilder.standard()
                .withRegion(s3region)
                .build();
    }

    public LinkedList<String> listFilesInFolder(String path) throws IOException {
        return listObjectsInFolder(path, false);
    }
    public LinkedList<String> listFoldersInFolder(String path) throws IOException {
        List<String> folders = listObjectsInFolder(path, true);
        Set<String> foldersSet = new HashSet<>(folders);
        return new LinkedList<>(foldersSet);
    }

    public LinkedList<String> listFolders(String path) throws IOException {
        LinkedList<String> fileNamesList =new LinkedList<>();
        boolean succeeded = false;
        String lastErrStr = "";
        LinkedList<String> folders = new LinkedList<>();
        for (int i=0; i<ioActionRetries; i++) {
            try {
                folders.clear();
                fileNamesList.clear();
                ListObjectsRequest listObjectRequest = new ListObjectsRequest().
                        withBucketName(bucketName).
                        withDelimiter(File.separator).
                        withPrefix(path);

                ObjectListing objectListing = s3client.listObjects(listObjectRequest);
                for (String commonPrefix : objectListing.getCommonPrefixes()) {
                    String[] keyParts = commonPrefix.split(File.separator);
                    if (keyParts == null || keyParts.length == 0)
                        continue;

                    String folderName = keyParts[keyParts.length-1];
                    folders.add(folderName);
                }
                succeeded =true;
                break;
            } catch (AmazonServiceException ase) {
                lastErrStr = AmazonServiceException2ErrMsg(ase);
                LOGGER.warn("Failed deleting object '" + path + "', trial number " + (i+1) + ": " + lastErrStr);
            } catch (AmazonClientException ace) {
                lastErrStr = AmazonClientException2ErrMsg(ace);
                LOGGER.warn("Failed deleting object '" + path + "', trial number " + (i+1) + ": " + lastErrStr);
            }
            try {
                if (i<ioActionRetries-1) //sleep between trials (but not after the last)
                    Thread.sleep(RETRY_INTERVAL_MS);
            } catch (InterruptedException e) {
                //do nothing
            }
        }
        return folders;
    }
    private LinkedList<String> listObjectsInFolder(String path, boolean folders) throws IOException {
        LinkedList<String> fileNamesList =new LinkedList<>();
        boolean succeeded = false;
        String lastErrStr = "";

        for (int i=0; i<ioActionRetries; i++) {
            try {
                fileNamesList.clear();
                ListObjectsRequest listObjectRequest = new ListObjectsRequest().
                        withBucketName(bucketName).
                        withPrefix(path);

                ObjectListing objectListing = s3client.listObjects(listObjectRequest);

                addObjectsNames(objectListing, fileNamesList, folders, path);

                while (objectListing.isTruncated()) {
                    objectListing = s3client.listNextBatchOfObjects(objectListing);
                    addObjectsNames(objectListing, fileNamesList, folders, path);
                }
                succeeded =true;
                break;
            } catch (AmazonServiceException ase) {
                lastErrStr = AmazonServiceException2ErrMsg(ase);
                LOGGER.warn("Failed deleting object '" + path + "', trial number " + (i+1) + ": " + lastErrStr);
            } catch (AmazonClientException ace) {
                lastErrStr = AmazonClientException2ErrMsg(ace);
                LOGGER.warn("Failed deleting object '" + path + "', trial number " + (i+1) + ": " + lastErrStr);
            }
            try {
                if (i<ioActionRetries-1) //sleep between trials (but not after the last)
                    Thread.sleep(RETRY_INTERVAL_MS);
            } catch (InterruptedException e) {
                //do nothing
            }
        }

        if (!succeeded) {
            String err = "Failed listing folder '" + path + "' objects: " + lastErrStr;
            LOGGER.error(err);
            throw new IOException(err);
        }
        return fileNamesList;
    }

    private void addObjectsNames(ObjectListing objectListing, LinkedList<String> fileNamesList, boolean folders, String path) {
        for (S3ObjectSummary objectSummary: objectListing.getObjectSummaries()) {
            String key = objectSummary.getKey();
            if (!folders) {
                if (key.equals(File.separator)) //ignore sub folders for now
                    continue;
            }


            String[] keyParts = key.split(File.separator);
            if (keyParts == null || keyParts.length == 0)
                continue;

            String fileName = keyParts[keyParts.length-1];

            if (fileName!=null && !fileName.isEmpty()) {
                if (folders) {
                    String subFolder = getNextSubFolderPath(key, path);
                    if (subFolder != null) {
                        fileNamesList.add(subFolder);
                    }
                } else {
                    fileNamesList.add(fileName);
                }
            }
        }

    }

    private String getNextSubFolderPath(String fileName, String path) {
        if (!path.endsWith(File.separator)) {
            path = path + File.separator;
        }
        int pathLoc = fileName.indexOf(path);
        if (pathLoc < 0) return null;
        String subString = fileName.substring(pathLoc+path.length(), fileName.length() -1);
        int seperatorLoc = subString.indexOf(File.separator);
        if (seperatorLoc < 0) return null;
        String subFolder = subString.substring(0, seperatorLoc);
        return path + subFolder;
    }

    public void copyFile(String path, String destinationBucket, String sourcePrefix) {
        CopyObjectRequest copyObjRequest = new CopyObjectRequest(bucketName, sourcePrefix, destinationBucket, path);
        for (int i=0; i<ioActionRetries; i++) {
            try {
                s3client.copyObject(copyObjRequest);
                return;
            } catch (SdkClientException e) {
                LOGGER.warn("failed copying file:"+e.getMessage());
                if (i==ioActionRetries-1) {
                    throw e;
                }
                try {
                    Thread.sleep(RETRY_INTERVAL_MS);
                } catch (InterruptedException interruptedException) {
                    //we don't care
                }
            }
        }

    }
    public void deleteFiles(String folder, List<String> fileNames, String skipFile) throws IOException {
        List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<DeleteObjectsRequest.KeyVersion>();

        for (String file:fileNames) {
            if (!file.endsWith(skipFile)) {
                keys.add(new DeleteObjectsRequest.KeyVersion(folder+file));
            }
            if (keys.size() == S3_MAX_DELETED_FILES) { //S3 can delete up to 1000 files at once
                doDeleteFiles(keys);
                keys.clear();
            }
        }

        if (keys.size()>0) {
            doDeleteFiles(keys);
        }
    }


    //try 3 times to delete the object
    public void doDeleteFiles(List<DeleteObjectsRequest.KeyVersion> keys) throws IOException {

        boolean succeeded = false;

        if (keys.size() == 0) {
            return;
        }

        String lastErrStr = "";
        for (int i=0; i<ioActionRetries; i++) {
            try {
                DeleteObjectsRequest request = new DeleteObjectsRequest(bucketName).withKeys(keys);
                DeleteObjectsResult result = s3client.deleteObjects(request);
                if (result.getDeletedObjects().size() == keys.size()) {
                    succeeded = true;
                    break;
                }
                else {
                    lastErrStr = "Failed deleting all objects.";
                    LOGGER.warn("Failed deleting all objects, trial number " + (i + 1));
                }
            } catch (AmazonServiceException ase) {
                lastErrStr = AmazonServiceException2ErrMsg(ase);
                LOGGER.warn("Failed deleting objects, trial number " + (i + 1) + ": " + lastErrStr);
            } catch (AmazonClientException ace) {
                lastErrStr = AmazonClientException2ErrMsg(ace);
                LOGGER.warn("Failed deleting object, trial number " + (i + 1) + ": " + lastErrStr);
            }
            try {
                if (i < ioActionRetries - 1) //sleep between trials (but not after the last)
                    Thread.sleep(RETRY_INTERVAL_MS);
            } catch (InterruptedException e) {
                //do nothing
            }
        }

        if (!succeeded) {
            String err = "Failed deleting objects: " + lastErrStr;
            LOGGER.error(err);
            throw new IOException(err);
        }
    }

    //try 3 times to delete the object
    public void deleteFile(String path) throws IOException {

        boolean succeeded = false;

        String lastErrStr = "";
        for (int i=0; i<ioActionRetries; i++) {
            try {
                s3client.deleteObject(bucketName, path);
                succeeded =true;
                break;
            } catch (AmazonServiceException ase) {
                lastErrStr = AmazonServiceException2ErrMsg(ase);
                LOGGER.warn("Failed deleting object '" + path + "', trial number " + (i+1) + ": " + lastErrStr);
            } catch (AmazonClientException ace) {
                lastErrStr = AmazonClientException2ErrMsg(ace);
                LOGGER.warn("Failed deleting object '" + path + "', trial number " + (i+1) + ": " + lastErrStr);
            }
            try {
                if (i<ioActionRetries-1) //sleep between trials (but not after the last)
                    Thread.sleep(RETRY_INTERVAL_MS);
            } catch (InterruptedException e) {
                //do nothing
            }
        }

        if (!succeeded) {
            String err = "Failed deleting object '" + path + "' : " + lastErrStr;
            LOGGER.error(err);
            throw new IOException(err);
        }
    }


    public String getFileContent(String path) throws IOException {
        if (!s3client.doesObjectExist(bucketName, path)) {
            return null;
        }
        S3Object o = s3client.getObject(bucketName, path);
        S3ObjectInputStream s3is = o.getObjectContent();
        String str = getAsString(s3is);
        return str;
    }

    public void writeFileContent(String path, String suffix, String key, String content) throws IOException {
        s3client.putObject(new PutObjectRequest(bucketName, key, createFile(path, suffix, content)));
    }

    private File createFile(String path, String suffix, String content) throws IOException {
        File file = File.createTempFile(path, suffix);
        file.deleteOnExit();

        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write(content);
        writer.close();

        return file;
    }

    public void addFullPermission(String path) {
        s3client.setObjectAcl(bucketName, path, CannedAccessControlList.BucketOwnerFullControl);
    }

    public void writeData(String path, String data) throws IOException {
        boolean succeeded = false;

        String lastErrStr = "";
        for (int i=0; i<ioActionRetries; i++) {
            try {
                doWriteData(path, data);
                succeeded =true;
                break;
            } catch (AmazonServiceException ase) {
                lastErrStr = AmazonServiceException2ErrMsg(ase);
                LOGGER.warn("Failed writing data to '" + path + "', trial number " + (i+1) + ": " +lastErrStr);
            } catch (AmazonClientException ace) {
                lastErrStr = AmazonClientException2ErrMsg(ace);
                LOGGER.warn("Failed writing data to '" + path + "', trial number " + (i+1) + ": " + lastErrStr);
            }
            try {
                if (i<ioActionRetries-1) //sleep between trials (but not after the last)
                    Thread.sleep(RETRY_INTERVAL_MS);
            } catch (InterruptedException e) {
                //do nothing
            }
        }

        if (!succeeded) {
            String err = "Failed writing data to '" + path + "' : " + lastErrStr;
            LOGGER.error(err);
            throw new IOException(err);
        }
    }

    private void doWriteData(String path, String data)  throws AmazonClientException {
        byte[] contentAsBytes = data.getBytes();
        ObjectMetadata          md = new ObjectMetadata();
        md.setContentLength(contentAsBytes.length);

        s3client.putObject(bucketName, path, new ByteArrayInputStream(contentAsBytes), md);
    }

    private static String getAsString(InputStream is) throws IOException {
        if (is == null)
            return "";
        StringBuilder sb = new StringBuilder();
        try {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(is, StringUtils.UTF8));
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        } finally {
            is.close();
        }
        return sb.toString();
    }

    private String AmazonClientException2ErrMsg (AmazonClientException ace) {
        return"Caught an AmazonClientException. Error Message: " + ace.getMessage();
    }

    private String AmazonServiceException2ErrMsg (AmazonServiceException ase) {
        return"Caught an AmazonServiceException. Error Message: " + ase.getMessage();
    }
}
