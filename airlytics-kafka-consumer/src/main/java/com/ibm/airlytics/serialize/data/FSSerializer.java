package com.ibm.airlytics.serialize.data;

import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import org.apache.commons.io.FileUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;


public class FSSerializer implements DataSerializer {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(FSSerializer.class.getName());

    public static final int RETRY_INTERVAL_MS = 1000;

    private String rootFolder;
    private int ioActionRetries;

    public FSSerializer(String rootFolder, int ioActionRetries) {
        this.rootFolder = rootFolder;
        if (!this.rootFolder.endsWith(File.separator)) {
            this.rootFolder = this.rootFolder+File.separator;
        }

        this.ioActionRetries = ioActionRetries;
    }

    public LinkedList<String> listFilesInFolder(String path, String fileNamePrefix, boolean returnFullPath) throws IOException {
        LinkedList<String> fileNamesList =new LinkedList<>();
        boolean succeeded = false;
        String lastErrStr = "";

        //if the folder does not exist - return empty list
        File folder = new File(rootFolder + path);
        if (!folder.exists()) {
            return fileNamesList;
        }

        for (int i=0; i<ioActionRetries; i++) {
            try {
                fileNamesList.clear();


                Collection<File> listFiles = FileUtils.listFiles(new File(rootFolder + path), null, true);

                addObjectsNames(listFiles, fileNamesList, fileNamePrefix, returnFullPath);

                succeeded =true;
                break;
            } catch (Exception e) {
                e.printStackTrace();
                lastErrStr = e.getMessage();
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

    public LinkedList<String> listFolders(String path) throws IOException {
        LinkedList<String> fileNamesList =new LinkedList<>();
        boolean succeeded = false;
        String lastErrStr = "";

        //if the folder does not exist - return empty list
        File folder = new File(rootFolder + path);
        if (!folder.exists()) {
            return fileNamesList;
        }

        LinkedList<String> folders = new LinkedList<>();
        for (int i=0; i<ioActionRetries; i++) {
            try {
                folders.clear();
                File[] directories = new File(rootFolder+path).listFiles(File::isDirectory);
                fileNamesList.clear();

                for (File d:directories) {
                    fileNamesList.add(d.getName());
                }

                succeeded =true;
                break;
            } catch (Exception e) {
                lastErrStr = e.getMessage();
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
            String err = "Failed listing folder '" + path + "' folders: " + lastErrStr;
            LOGGER.error(err);
            throw new IOException(err);
        }

        return folders;
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

    private void addObjectsNames(Collection<File> filesList, LinkedList<String> fileNamesList, String fileNamePrefix, boolean returnFullPath) {
        for (File f:filesList) {
            if (f.isDirectory()) //ignore sub folders for now
                continue;

            String fileName = f.getName();

            if (fileNamePrefix!=null && !fileNamePrefix.isEmpty()&& !fileName.startsWith(fileNamePrefix)) {
                continue;
            }

            if (fileName!=null && !fileName.isEmpty()) {
                fileNamesList.add(returnFullPath?f.getAbsolutePath():fileName);
            }
        }
    }

    //return the size of all files in the given folder. Skipping the skipFile if specified.
    //Sum only the sizes of the files with the given extension if given.
    public double getFolderSizeM(String folder, String skipFile, String requestedExtension) throws IOException {
        File f =  new File(rootFolder + folder);
        if (!f.exists()) {
            return 0;
        }

        double totalSize = 0;
        String lastErrStr = "";

        boolean succeeded = false;

        for (int i=0; i<ioActionRetries; i++) {
            try {
                totalSize=0;
                Collection<File> listFiles = FileUtils.listFiles(new File(rootFolder + folder), null, true);

                totalSize += calcObjectsSize(listFiles, skipFile, requestedExtension);
                
                succeeded =true;
                break;
            } catch (Exception e) {
                e.printStackTrace();
                lastErrStr = e.getMessage();
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

    private long calcObjectsSize(Collection<File> filesList, String skipFile, String requestedExtension) {
        long size = 0;
        for (File f:filesList) {
            if (f.isDirectory()) //ignore sub folders for now
                continue;

            String fileName = f.getName();

            if (skipFile!=null && fileName.endsWith(skipFile))
                continue;

            if (requestedExtension!=null && !fileName.endsWith(requestedExtension)) {
                continue;
            }

            if (fileName!=null && !fileName.isEmpty()) {
                size += f.length();
            }
        }
        return size;
    }

    public double getFileSizeB(String path, boolean ignorePrefix) {
        File f = new File((ignorePrefix?"":rootFolder) + path);
        if (!f.exists()) {
            return 0;
        }
        return f.length();
    }

    public double getFileSizeBIgnorePrefix(String path) {
        File f = new File(path);
        if (!f.exists()) {
            return 0;
        }
        return f.length();
    }

    public void deleteFiles(String folderPath, List<String> fileNames, String skipFile, boolean deleteFolderIfEmpty) throws IOException {
        for (String file:fileNames) {
            if (skipFile==null || !file.endsWith(skipFile)) {
                File f = new File(rootFolder + File.separator + folderPath + file);
                deleteFile(folderPath + file);
            }
        }
        if (deleteFolderIfEmpty) {
            File folder = new File(rootFolder + folderPath);
            if (folder.exists() && folder.isDirectory()) {
                folder.delete(); //will be deleted only if empty
            }
        }
    }

    public void deleteFilesIgnoreError(String folderPath, List<String> fileNames, String skipFile, boolean deleteFolderIfEmpty) {
        for (String file:fileNames) {
            if (skipFile==null || !file.endsWith(skipFile)) {
                String path = rootFolder + File.separator + folderPath + file;

                File f = new File(path);
                if (!f.exists()) {
                    LOGGER.info("trying to delete a non existing file:" + path + ". Ignoring.");
                    return;
                }

                for (int i=0; i<ioActionRetries; i++) {
                    try {
                        if (f.delete())
                            break;
                    } catch (Exception e) {
                        LOGGER.info("Failed deleting object '" + path + "', trial number " + (i+1));
                    }

                    try {
                        if (i<ioActionRetries-1) //sleep between trials (but not after the last)
                            Thread.sleep(RETRY_INTERVAL_MS);
                    } catch (InterruptedException e) {
                        //do nothing
                    }
                }
            }
        }

        if (deleteFolderIfEmpty) {
            File folder = new File(rootFolder + folderPath);
            if (folder.exists() && folder.isDirectory()) {
                folder.delete(); //will be deleted only if empty
            }
        }
    }

    //try 3 times to delete the object
    public void deleteFile(String path) throws IOException {
        boolean succeeded = false;

        File f = new File(rootFolder+File.separator+path);

        String lastErrStr = "";
        for (int i=0; i<ioActionRetries; i++) {
            if (!f.exists()) {
                LOGGER.info("trying to delete a non existing file:" + path + ". Ignoring.");
                return;
            }

            try {
                succeeded =f.delete();
                if (succeeded)
                    break;
            } catch (Exception e) {
                lastErrStr = e.getMessage();
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

    public boolean exists(String path) {
        File f = new File(rootFolder+File.separator+path);
        return f.exists();
    }

    public void addFullPermission(String path) throws IOException{
        File file = new File(rootFolder+path);
        Set<PosixFilePermission> perms = new HashSet<>();
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);

        Files.setPosixFilePermissions(file.toPath(), perms);
    }

    public void writeData(String path, String data) throws IOException {
        writeData(path, data, false);
    }

    public void writeData(String path, String data, boolean append) throws IOException {
        boolean succeeded = false;

        String lastErrStr = "";
        for (int i=0; i<ioActionRetries; i++) {
            try {
                doWriteData(path, data, append);
                succeeded =true;
                break;
            } catch (Exception e) {
                lastErrStr = e.getMessage();
                LOGGER.warn("Failed writing data to '" + path + "', trial number " + (i+1) + ": " +lastErrStr);
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

    private void doWriteData(String path, String data, boolean append)  throws IOException {
        FileUtils.write(new File(rootFolder + path), data, Charset.forName("UTF-8"), append);
    }

    public String readData(String path) throws IOException {
        return FileUtils.readFileToString(new File(rootFolder + path), Charset.forName("UTF-8"));
    }


}
