package com.ibm.airlytics.serialize.data;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.IOUtils;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;


public class S3Serializer implements DataSerializer {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(S3Serializer.class.getName());

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

    public LinkedList<String> listFilesInFolder(String path, String fileNamePrefix, boolean returnFullPath) throws IOException {
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

                addObjectsNames(objectListing, fileNamesList, fileNamePrefix, returnFullPath);

                while (objectListing.isTruncated()) {
                    objectListing = s3client.listNextBatchOfObjects(objectListing);
                    addObjectsNames(objectListing, fileNamesList, fileNamePrefix, returnFullPath);
                }
                succeeded =true;
                break;
            } catch (AmazonServiceException ase) {
                ase.printStackTrace();
                lastErrStr = AmazonServiceException2ErrMsg(ase);
                LOGGER.warn("Failed listing object '" + path + "', trial number " + (i+1) + ": " + lastErrStr);
            } catch (AmazonClientException ace) {
                ace.printStackTrace();
                lastErrStr = AmazonClientException2ErrMsg(ace);
                LOGGER.warn("Failed listing object '" + path + "', trial number " + (i+1) + ": " + lastErrStr);
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

    public double getFileSizeB(String path, boolean ignorePrefix) throws IOException {
        for (int i=0; i<ioActionRetries; i++) {
            try {
                return s3client.getObjectMetadata(bucketName, path).getContentLength();
            } catch (Exception e) {
                if (i<ioActionRetries-1) {//sleep between trials (but not after the last)
                    try {
                        Thread.sleep(RETRY_INTERVAL_MS);
                    } catch (InterruptedException ie) {
                        //do nothing
                    }
                } else {
                    String err = "Fail calculating file '" + path + "' size: " + e.getMessage();
                    LOGGER.error(err); 
                    throw new IOException(err);
                }
            }
        }

        return 0;
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

            if (!succeeded) {
                String err = "Failed listing folder '" + path + "' folders: " + lastErrStr;
                LOGGER.error(err);
                throw new IOException(err);
            }
        }
        return folders;
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

    private void addObjectsNames(ObjectListing objectListing, LinkedList<String> fileNamesList, String fileNamePrefix, boolean returnFullPath) {
        for (S3ObjectSummary objectSummary: objectListing.getObjectSummaries()) {
            String key = objectSummary.getKey();
            if (key.equals(File.separator)) //ignore sub folders for now
                continue;

            String[] keyParts = key.split(File.separator);
            if (keyParts == null || keyParts.length == 0)
                continue;

            String fileName = keyParts[keyParts.length-1];

            if (fileNamePrefix!=null && !fileNamePrefix.isEmpty()&& !fileName.startsWith(fileNamePrefix)) {
                continue;
            }

            if (fileName!=null && !fileName.isEmpty()) {
                fileNamesList.add(returnFullPath?key:fileName);
            }
        }
    }

    //return the size of all files in the given folder. Skipping the skipFile if specified.
    //Sum only the sizes of the files with the given extension if given.
    public double getFolderSizeM(String folder, String skipFile, String requestedExtension) throws IOException {
        double totalSize = 0;
        String lastErrStr = "";

        boolean succeeded = false;

        for (int i=0; i<ioActionRetries; i++) {
            try {
                totalSize=0;
                ListObjectsRequest listObjectRequest = new ListObjectsRequest().
                        withBucketName(bucketName).
                        withPrefix(folder);

                ObjectListing objectListing = s3client.listObjects(listObjectRequest);
                totalSize += calcObjectsSize(objectListing, skipFile, requestedExtension);

                while (objectListing.isTruncated()) {
                    objectListing = s3client.listNextBatchOfObjects(objectListing);
                    totalSize += calcObjectsSize(objectListing, skipFile, requestedExtension);
                }

                succeeded =true;
                break;
            } catch (AmazonServiceException ase) {
                ase.printStackTrace();
                lastErrStr = AmazonServiceException2ErrMsg(ase);
                LOGGER.warn("Failed listing object '" + folder + "', trial number " + (i+1) + ": " + lastErrStr);
            } catch (AmazonClientException ace) {
                ace.printStackTrace();
                lastErrStr = AmazonClientException2ErrMsg(ace);
                LOGGER.warn("Failed listing object '" + folder + "', trial number " + (i+1) + ": " + lastErrStr);
            }
            try {
                if (i<ioActionRetries-1) //sleep between trials (but not after the last)
                    Thread.sleep(RETRY_INTERVAL_MS);
            } catch (InterruptedException e) {
                //do nothing
            }
        }

         if (!succeeded) {
            String err = "Failed calculating folder size: " + lastErrStr;
            LOGGER.error(err);
            throw new IOException(err);
         }

         totalSize =  totalSize/(1024*1024);

         return Math.round(totalSize * 100) / 100.0; //round size to 2 decimal digits
    }

    private long calcObjectsSize(ObjectListing objectListing, String skipFile, String requestedExtension) {
        long size = 0;
        for (S3ObjectSummary objectSummary: objectListing.getObjectSummaries()) {
            String key = objectSummary.getKey();
            if (key.equals(File.separator)) //ignore sub folders for now
                continue;

            String[] keyParts = key.split(File.separator);
            if (keyParts == null || keyParts.length == 0)
                continue;

            String fileName = keyParts[keyParts.length-1];

            if (skipFile!=null && fileName.endsWith(skipFile))
                continue;

            if (requestedExtension!=null && !fileName.endsWith(requestedExtension)) {
                continue;
            }

            if (fileName!=null && !fileName.isEmpty()) {
                //System.out.println("fileName = " + fileName + " , size = " + objectSummary.getSize());
                size += objectSummary.getSize();
            }
        }
        //System.out.println("size = " + size);
        return size;
    }


    /**
     * Returns a given file in S3 as a stream
     * Important a caller is responsible to close the stream
     *
     * @param bucketName
     * @param fileName
     * @return
     */
    public InputStream readFileAsInputStream(String bucketName, String fileName)  {
        S3Object object = s3client.getObject(new GetObjectRequest(bucketName, fileName));
        return object.getObjectContent();
    }

    public void deleteFiles(String folderPath, List<String> fileNames, String skipFile, boolean deleteFolderIfEmpty) throws IOException {
        List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<DeleteObjectsRequest.KeyVersion>();

        for (String file:fileNames) {
            if (skipFile==null || !file.endsWith(skipFile)) {
                keys.add(new DeleteObjectsRequest.KeyVersion(folderPath+file));
            }
            if (keys.size() == S3_MAX_DELETED_FILES) { //S3 can delete up to 1000 files at once
                doDeleteFiles(keys, folderPath);
                keys.clear();
            }
        }

        if (keys.size()>0) {
            doDeleteFiles(keys, folderPath);
        }

        //in S3 - an empty folder is automatically deleted
    }


    public void deleteFilesIgnoreError(String folderPath, List<String> fileNames, String skipFile, boolean deleteFolderIfEmpty) {
        List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<DeleteObjectsRequest.KeyVersion>();

        for (String file:fileNames) {
            if (skipFile==null || !file.endsWith(skipFile)) {
                keys.add(new DeleteObjectsRequest.KeyVersion(folderPath+file));
            }
            if (keys.size() == S3_MAX_DELETED_FILES) { //S3 can delete up to 1000 files at once
                doDeleteFilesIgnoreError(keys, folderPath);
                keys.clear();
            }
        }

        if (keys.size()>0) {
            doDeleteFilesIgnoreError(keys, folderPath);
        }

        //in S3 - an empty folder is automatically deleted
    }

    //try 3 times to delete the object
    public void doDeleteFiles(List<DeleteObjectsRequest.KeyVersion> keys, String folder) throws IOException {

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
                    LOGGER.warn("Failed deleting all objects from folder " + folder + ", trial number " + (i + 1));
                }
            } catch (AmazonServiceException ase) {
                lastErrStr = AmazonServiceException2ErrMsg(ase);
                LOGGER.warn("Failed deleting objects from folder " + folder + ", trial number " + (i + 1) + ": " + lastErrStr);
            } catch (AmazonClientException ace) {
                lastErrStr = AmazonClientException2ErrMsg(ace);
                LOGGER.warn("Failed deleting object from folder " + folder + ", trial number " + (i + 1) + ": " + lastErrStr);
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
    public void doDeleteFilesIgnoreError(List<DeleteObjectsRequest.KeyVersion> keys, String folder) {

        if (keys.size() == 0) {
            return;
        }

        for (int i=0; i<ioActionRetries; i++) {
            try {
                DeleteObjectsRequest request = new DeleteObjectsRequest(bucketName).withKeys(keys);
                DeleteObjectsResult result = s3client.deleteObjects(request);
                if (result.getDeletedObjects().size() == keys.size()) {
                    break;
                }
                else {
                    LOGGER.info("Failed deleting all objects from folder " + folder + ", trial number " + (i + 1));
                }
            } catch (Exception e) {
                LOGGER.info("Failed deleting objects from folder " + folder + ", trial number " + (i + 1));
            }

            try {
                if (i < ioActionRetries - 1) //sleep between trials (but not after the last)
                    Thread.sleep(RETRY_INTERVAL_MS);
            } catch (InterruptedException e) {
                //do nothing
            }
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

    public void writeData(String path, String data, boolean append) throws IOException {

        if(append) {
            throw new RuntimeException("Operation is not supported");
        }
        writeData(path, data);
    }

    private void doWriteData(String path, String data)  throws AmazonClientException {
        byte[] contentAsBytes = data.getBytes();
        ObjectMetadata          md = new ObjectMetadata();
        md.setContentLength(contentAsBytes.length);

        s3client.putObject(bucketName, path, new ByteArrayInputStream(contentAsBytes), md);
    }

    private String AmazonClientException2ErrMsg (AmazonClientException ace) {
        return"Caught an AmazonClientException. Error Message: " + ace.getMessage();
    }

    private String AmazonServiceException2ErrMsg (AmazonServiceException ase) {
        return"Caught an AmazonServiceException. Error Message: " + ase.getMessage();
    }

    public boolean exists(String path) {
        try {
            s3client.getObjectMetadata(bucketName, path);
        } catch(AmazonServiceException e) {
            return false;
        }
        return true;
    }

    public String readData(String path)  throws IOException {
        try {
            S3Object res = doReadS3Object(path, ioActionRetries);

            InputStream is = res.getObjectContent();

            return IOUtils.toString(is);
        } catch (IOException ioe) {
            String err = "Failed reading String data from " + path + ": " + ioe.getMessage();
            throw new IOException(err);
        }
    }

    private S3Object doReadS3Object (String path, int numberOfTrials) throws IOException {
        String lastErrStr = "";
        for (int i=0; i<numberOfTrials; i++) {
            try {
                return s3client.getObject(bucketName, path);
            } catch (AmazonServiceException ase) {
                lastErrStr = AmazonServiceException2ErrMsg(ase);
                LOGGER.warn("Failed reading data from '" + path + "', trial number " + (i+1) + ": " + lastErrStr);

            } catch (AmazonClientException ace) {
                lastErrStr = AmazonClientException2ErrMsg(ace);
                LOGGER.warn("Failed reading data from '" + path + "', trial number " + (i+1) + ": " + lastErrStr);
            }

            try {
                if (i<numberOfTrials-1) //sleep between trials (but not after the last)
                    Thread.sleep(RETRY_INTERVAL_MS);
            } catch (InterruptedException e) {
                //do nothing
            }
        }

        String err = "Failed reading data from '" + path + "' : " + lastErrStr;
        LOGGER.error(err);
        throw new IOException(err);
    }

}
