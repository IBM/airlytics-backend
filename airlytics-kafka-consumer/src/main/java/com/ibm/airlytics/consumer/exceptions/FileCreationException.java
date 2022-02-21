package com.ibm.airlytics.consumer.exceptions;

import java.io.IOException;

public class FileCreationException extends IOException { public FileCreationException(String s) {
    super(s);
}

    public FileCreationException(String s, Exception e) {
        super(s, e);
    }
}
