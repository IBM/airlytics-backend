package com.ibm.airlytics.serialize.data;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public interface DataSerializer {
    public LinkedList<String> listFilesInFolder(String path, String fileNamePrefix, boolean returnFullPath) throws IOException;
    //public LinkedList<String> listFoldersInFolder(String path) throws IOException;
    public LinkedList<String> listFolders(String path) throws IOException;
    public void deleteFiles(String folderPath, List<String> fileNames, String skipFile, boolean deleteFolderIfEmpty) throws IOException;
    public void deleteFile(String path) throws IOException;
    public void deleteFilesIgnoreError(String folderPath, List<String> fileNames, String skipFile, boolean deleteFolderIfEmpty);
    public double getFolderSizeM(String folder, String skipFile, String requestedExtension) throws IOException;
    public double getFileSizeB(String path, boolean ignorePrefix) throws IOException;
    public void writeData(String path, String data) throws IOException;
    public void writeData(String path, String data, boolean append) throws IOException;
    public void addFullPermission(String path) throws IOException;
    public boolean exists (String path);
    public String readData(String path) throws IOException;
}
