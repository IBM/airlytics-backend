package com.ibm.airlytics.retentiontrackerqueryhandler.db.data;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public interface DataSerializer {
    public LinkedList<String> listFilesInFolder(String path) throws IOException;
    public LinkedList<String> listFoldersInFolder(String path) throws IOException;
    public LinkedList<String> listFolders(String path) throws IOException;
    public void deleteFiles(String folder, List<String> fileNames, String skipFile) throws IOException;
    public void deleteFile(String path) throws IOException;
    public void addFullPermission(String path) throws IOException;
    public void writeData(String path, String data) throws IOException;
    public String getFileContent(String path) throws IOException;
    public void writeFileContent(String path, String suffix, String key, String content) throws IOException;
    public void copyFile(String path, String sourceBucket, String sourcePrefix);
}
