package com.ibm.airlytics.retentiontrackerqueryhandler.db;

import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.data.DataSerializer;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.data.S3Serializer;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;

@Component
@DependsOn({"ConfigurationManager","Airlock"})
public class DumpBookmark {
    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(DumpBookmark.class.getName());
    private DataSerializer dataSerializer;
    private DumpBookmarkConfig config;

    public DumpBookmark() throws IOException {
        config = new DumpBookmarkConfig();
        config.initWithAirlock();
        this.dataSerializer = new S3Serializer(config.getS3region(), config.getS3Bucket(), config.getIoActionRetries());
    }

    public String getCursor() throws IOException {
        String cursor = dataSerializer.getFileContent(config.getS3RootFolder()+ File.separator+config.getCursorFileName());
        return cursor;
    }

    public void setCursor(String newCursor) throws IOException {
        String file = config.getCursorFileName();
        String suffix = ".txt";
        dataSerializer.writeFileContent(config.getS3RootFolder()+File.separator+file, suffix, config.getS3RootFolder()+File.separator+file, newCursor);
    }
}
