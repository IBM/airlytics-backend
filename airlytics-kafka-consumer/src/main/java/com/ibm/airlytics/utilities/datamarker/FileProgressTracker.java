package com.ibm.airlytics.utilities.datamarker;

import java.util.HashSet;
import java.util.Set;

public class FileProgressTracker {
    private FileMarkers prodMarkers = new FileMarkers();
    private FileMarkers devMarkers = new FileMarkers();
    private Set<String> progressFilesToDelete = new HashSet<>();

    public FileMarkers getProdMarkers() {
        return prodMarkers;
    }

    public FileMarkers getDevMarkers() {
        return devMarkers;
    }

    public Set<String> getProgressFilesToDelete() {
        return progressFilesToDelete;
    }
}
