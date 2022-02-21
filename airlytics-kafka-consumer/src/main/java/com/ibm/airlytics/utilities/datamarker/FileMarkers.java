package com.ibm.airlytics.utilities.datamarker;

import java.util.HashMap;
import java.util.Map;

public class FileMarkers {
    public Map<String, Integer> getLastSentEvent() {
        return lastSentEvent;
    }

    public Map<String, Integer> getCurrentSentEvent() {
        return currentSentEvent;
    }

    Map<String, Integer> lastSentEvent = new HashMap<>(); //map between product and the last prod event sent before the prev consumer run terminated unexpectedly
    Map<String, Integer> currentSentEvent = new HashMap<>(); //map between product and the current prod event sent to the proxy
}
