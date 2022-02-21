package com.ibm.airlytics.utilities.datamarker;

import com.ibm.airlytics.serialize.data.DataSerializer;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ReadProgressMarker {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(ReadProgressMarker.class.getName());

    public static final String S3_FILE_SEPARATOR = "/";

    private DataSerializer progressDataSerializer; //  /usr/src/app/data/airlytics-datalake-prod/ltvProgress

    public ReadProgressMarker(DataSerializer progressDataSerializer) {
        this.progressDataSerializer = progressDataSerializer;
    }

    public Integer getLastRecordSent(FileProgressTracker fileTracker, boolean devEvents, String prodId, String progressFileName) throws IOException {
        FileMarkers markers = devEvents? fileTracker.getDevMarkers() : fileTracker.getProdMarkers();
        Map<String, Integer> lastSentEventMap = markers.getLastSentEvent();
        Integer lastRecordSent = lastSentEventMap.get(prodId);
        if (lastRecordSent == null) {
            //first time a batch is sent for this progress file (file+prod+dev/prod) in this consumer execution
            if (progressDataSerializer.exists(progressFileName)) {
                String lastSendRecordStr = progressDataSerializer.readData(progressFileName);
                if (lastSendRecordStr==null || lastSendRecordStr.isEmpty()) {
                    lastRecordSent = -1;
                    LOGGER.warn("***** lastRecordSent for file "+progressFileName+" is empty");
                } else {
                    lastRecordSent = Integer.valueOf(lastSendRecordStr);
                    fileTracker.getProgressFilesToDelete().add(progressFileName);
                    LOGGER.info("***** lastRecordSent '" + lastRecordSent + "' found in file :" + progressFileName);
                }
            } else {
                lastRecordSent = -1;
            }
            lastSentEventMap.put(prodId, lastRecordSent);
        }
        return lastRecordSent;
    }

    public Integer getCurrentEvent(FileProgressTracker fileTracker, String prodId, boolean devEvents) {
        FileMarkers markers = devEvents? fileTracker.getDevMarkers() : fileTracker.getProdMarkers();
        Integer currentEvent = markers.getCurrentSentEvent().get(prodId);
        if (currentEvent == null) {
            currentEvent = 0;
        }
        return currentEvent;
    }

    public boolean shouldSkipBatch(FileProgressTracker fileTracker, boolean devEvents, String prodId, int batchSize, Integer lastRecordSent, Integer currentEvent) {
        FileMarkers markers = devEvents? fileTracker.getDevMarkers() : fileTracker.getProdMarkers();
        if (lastRecordSent > -1 && lastRecordSent >= (currentEvent + batchSize)) {
            currentEvent += batchSize;
            markers.getCurrentSentEvent().put(prodId, currentEvent);
            return true; //skip the whole batch
        }
        return false;
    }

    public void markBatch(FileProgressTracker fileTracker, boolean devEvents, String prodId, Integer currentEvent, String progressFileName) throws IOException {
        FileMarkers markers = devEvents? fileTracker.getDevMarkers() : fileTracker.getProdMarkers();
        markers.getCurrentSentEvent().put(prodId, currentEvent);

        fileTracker.getProgressFilesToDelete().add(progressFileName);
        progressDataSerializer.writeData(progressFileName, String.valueOf(currentEvent));
    }

    public String getProgressFileName(String filePath, String prodId, boolean dev) {
        String env = dev ? "dev" : "prod";
        return prodId + "XX" + env + "XX" + filePath.replace(S3_FILE_SEPARATOR, "XX");
    }

    public void deleteFiles(List<String> progressFilesToDelete) throws IOException {
        progressDataSerializer.deleteFiles("", progressFilesToDelete, null, false);
    }
}
