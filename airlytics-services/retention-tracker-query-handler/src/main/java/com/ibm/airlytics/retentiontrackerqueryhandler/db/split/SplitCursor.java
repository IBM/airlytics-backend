package com.ibm.airlytics.retentiontrackerqueryhandler.db.split;

import com.ibm.airlytics.retentiontrackerqueryhandler.db.data.S3Serializer;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.data.DataSerializer;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;

@Component
@DependsOn({"ConfigurationManager","Airlock"})
public class SplitCursor {
    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(SplitCursor.class.getName());
    private DataSerializer dataSerializer;
    private SplitCursorConfig config;

    public SplitCursor() throws IOException {
        config = new SplitCursorConfig();
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
