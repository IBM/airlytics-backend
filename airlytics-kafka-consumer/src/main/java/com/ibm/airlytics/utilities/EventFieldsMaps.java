package com.ibm.airlytics.utilities;

import com.fasterxml.jackson.databind.JsonNode;
import com.ibm.airlytics.consumer.persistence.FieldConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.*;

public class EventFieldsMaps {

    //Map<event->Map<fieldName->Map<version, fieldConfig>>>
    private HashMap<String, HashMap<String, HashMap<String, FieldConfig>>> configuredEventsFieldConfigs = new HashMap<>();

    //Map<fieldName->Map<version, fieldConfig>>
    private HashMap<String, HashMap<String,FieldConfig>> configuredCommonFieldConfigs = new HashMap<>();

    //Map<fieldName->Map<version, fieldConfig>>
    private HashMap<String, HashMap<String,FieldConfig>> configuredCustomDimensionsFieldConfigs = new HashMap<>();

    //map between the event and its piMarker field
    private HashMap<String, String> eventsWithPIMarker = new HashMap<String, String>();

    public EventFieldsMaps() {
    }

    public HashMap<String, HashMap<String, HashMap<String, FieldConfig>>> getConfiguredEventsFieldConfigs() {
        return configuredEventsFieldConfigs;
    }

    public void setConfiguredEventsFieldConfigs(HashMap<String, HashMap<String, HashMap<String, FieldConfig>>> configuredEventsFieldConfigs) {
        this.configuredEventsFieldConfigs = configuredEventsFieldConfigs;
    }

    public HashMap<String, HashMap<String, FieldConfig>> getConfiguredCommonFieldConfigs() {
        return configuredCommonFieldConfigs;
    }

    public void setConfiguredCommonFieldConfigs(HashMap<String, HashMap<String, FieldConfig>> configuredCommonFieldConfigs) {
        this.configuredCommonFieldConfigs = configuredCommonFieldConfigs;
    }

    public HashMap<String, HashMap<String, FieldConfig>> getConfiguredCustomDimensionsFieldConfigs() {
        return configuredCustomDimensionsFieldConfigs;
    }

    public void setConfiguredCustomDimensionsFieldConfigs(HashMap<String, HashMap<String, FieldConfig>> configuredCustomDimensionsFieldConfigs) {
        this.configuredCustomDimensionsFieldConfigs = configuredCustomDimensionsFieldConfigs;
    }

    public HashMap<String, String> getEventsWithPIMarker() {
        return eventsWithPIMarker;
    }

    public void setEventsWithPIMarker(HashMap<String, String> eventsWithPIMarker) {
        this.eventsWithPIMarker = eventsWithPIMarker;
    }

    public void buildConfiguredFieldsMaps(HashMap<String, FieldConfig> commonFieldTypesMap, HashMap<String, HashMap<String, FieldConfig>> eventsFieldTypesMap, HashMap<String, FieldConfig> customDimensionsFieldTypesMap) {
        configuredCommonFieldConfigs.clear();
        configuredEventsFieldConfigs.clear();
        configuredCustomDimensionsFieldConfigs.clear();
        eventsWithPIMarker.clear();

        //build common fields map
        Set<String> commonFieldNames = commonFieldTypesMap.keySet();
        for (String commonFieldName : commonFieldNames) {
            if (commonFieldName.contains(VERSION_DELIMITER)) {
                int pos = commonFieldName.indexOf(VERSION_DELIMITER);
                String name = commonFieldName.substring(0, pos);
                String version = commonFieldName.substring(pos+2);
                if (version.isEmpty()) {
                    throw new IllegalArgumentException("Illegal common field name "  + commonFieldName + ". No version specified");
                }
                if (name.isEmpty()) {
                    throw new IllegalArgumentException("Illegal common field name "  + commonFieldName + ". Only version specified");
                }
                String parquetFieldName = commonFieldTypesMap.get(commonFieldName).getParquetField();
                if(parquetFieldName==null || parquetFieldName.isEmpty()) {
                    throw new IllegalArgumentException("Illegal common field name "  + commonFieldName + ". Versioned events must contain parquetField name.");
                }

                addFieldToCommonFieldsMap(name, version, commonFieldTypesMap.get(commonFieldName));
            }
            else { //default
                addFieldToCommonFieldsMap(commonFieldName, DEFAULT_VERSION, commonFieldTypesMap.get(commonFieldName));
            }
        }

        //go over the created map and verify that no field is created without default version
        Set<String> fieldNames = configuredCommonFieldConfigs.keySet();
        for (String fieldName : fieldNames) {
            Set<String> fieldVersions = configuredCommonFieldConfigs.get(fieldName).keySet();
            if (!fieldVersions.contains(DEFAULT_VERSION)) {
                throw new IllegalArgumentException("Illegal common field "  + fieldName + ". Default version is not configured.");
            }
        }


        //build events fields map
        Set<String> eventNames = eventsFieldTypesMap.keySet();
        for (String eventName : eventNames) {
            Map<String, FieldConfig> eventFieldsMap = eventsFieldTypesMap.get(eventName);
            Set<String> eventFieldNames = eventFieldsMap.keySet();
            for (String eventFieldName : eventFieldNames) {
                if (eventFieldName.contains(VERSION_DELIMITER)) {
                    int pos = eventFieldName.indexOf(VERSION_DELIMITER);
                    String name = eventFieldName.substring(0, pos);
                    String version = eventFieldName.substring(pos+2);
                    if (version.isEmpty()) {
                        throw new IllegalArgumentException("Illegal event '" + eventName+ "' field name "  + eventFieldName + ". No version specified");
                    }
                    if (name.isEmpty()) {
                        throw new IllegalArgumentException("Illegal '" + eventName + "' field name "  + eventFieldName + ". Only version specified");
                    }

                    String parquetFieldName =  eventsFieldTypesMap.get(eventName).get(eventFieldName).getParquetField();
                    if(parquetFieldName==null || parquetFieldName.isEmpty()) {
                        throw new IllegalArgumentException("Illegal field name '"  + eventFieldName + "' in event '" + eventName+ "'. Versioned events must contain parquetField name.");
                    }

                    addFieldToEventFieldsMap(eventName, name, version, eventsFieldTypesMap.get(eventName).get(eventFieldName));
                }
                else { //default
                    addFieldToEventFieldsMap(eventName, eventFieldName, DEFAULT_VERSION, eventsFieldTypesMap.get(eventName).get(eventFieldName));
                }


                boolean isPiMarker = eventsFieldTypesMap.get(eventName).get(eventFieldName).isPiMarker();
                 if (isPiMarker) {
                    //validate that is boolean
                    if (!eventsFieldTypesMap.get(eventName).get(eventFieldName).getType().equals(FieldConfig.FIELD_TYPE.BOOLEAN)){
                        throw new IllegalArgumentException("Illegal field '" + eventFieldName + "' in event '" + eventName + "'. piMarker field exists with the wrong type. piMarker field should be BOOLEAN");
                    }

                    if (eventsWithPIMarker.containsKey(eventName)) {
                        throw new IllegalArgumentException("Illegal event '" + eventName + "'. Contains more than one piMarker fields");
                    }
                    eventsWithPIMarker.put(eventName, eventFieldName);
                }
            }
            //go over the created map for the event and verify that no event field is created without default version
            fieldNames = configuredEventsFieldConfigs.get(eventName).keySet();
            for (String fieldName : fieldNames) {
                Set<String> fieldVersions = configuredEventsFieldConfigs.get(eventName).get(fieldName).keySet();
                if (!fieldVersions.contains(DEFAULT_VERSION)) {
                    throw new IllegalArgumentException("Illegal field '" + fieldName + "' in event '" + eventName + "'. Default version is not configured.");
                }
            }
        }

        //build customDimensions fields map
        Set<String> customDimensionsFieldNames = customDimensionsFieldTypesMap.keySet();
        for (String customDimensionsFieldName : customDimensionsFieldNames) {
            if (customDimensionsFieldName.contains(VERSION_DELIMITER)) {
                int pos = customDimensionsFieldName.indexOf(VERSION_DELIMITER);
                String name = customDimensionsFieldName.substring(0, pos);
                String version = customDimensionsFieldName.substring(pos+2);
                if (version.isEmpty()) {
                    throw new IllegalArgumentException("Illegal customDimensions field name "  + customDimensionsFieldName + ". No version specified");
                }
                if (name.isEmpty()) {
                    throw new IllegalArgumentException("Illegal customDimensions field name "  + customDimensionsFieldName + ". Only version specified");
                }
                String parquetFieldName = customDimensionsFieldTypesMap.get(customDimensionsFieldName).getParquetField();
                if(parquetFieldName==null || parquetFieldName.isEmpty()) {
                    throw new IllegalArgumentException("Illegal customDimensions field name "  + customDimensionsFieldName + ". Versioned events must contain parquetField name.");
                }

                addFieldToCustomDimensionsFieldsMap(name, version, customDimensionsFieldTypesMap.get(customDimensionsFieldName));
            }
            else { //default
                addFieldToCustomDimensionsFieldsMap(customDimensionsFieldName, DEFAULT_VERSION, customDimensionsFieldTypesMap.get(customDimensionsFieldName));
            }
        }

        //go over the created map and verify that no field is created without default version
        Set<String>  cdFieldNames = configuredCustomDimensionsFieldConfigs.keySet();
        for (String cdFieldName : cdFieldNames) {
            Set<String> fieldVersions = configuredCustomDimensionsFieldConfigs.get(cdFieldName).keySet();
            if (!fieldVersions.contains(DEFAULT_VERSION)) {
                throw new IllegalArgumentException("Illegal customDimensions field "  + cdFieldName + ". Default version is not configured.");
            }
        }
    }

    private void addFieldToCommonFieldsMap(String fieldName, String version, FieldConfig fieldConfig) {
        HashMap<String,FieldConfig> fieldVersionsMap = configuredCommonFieldConfigs.get(fieldName);
        if (fieldVersionsMap == null) {
            fieldVersionsMap = new HashMap<String,FieldConfig>();
            configuredCommonFieldConfigs.put(fieldName, fieldVersionsMap);
        }

        fieldVersionsMap.put(version, fieldConfig);
    }

    private void addFieldToCustomDimensionsFieldsMap(String fieldName, String version, FieldConfig fieldConfig) {
        HashMap<String,FieldConfig> fieldVersionsMap = configuredCustomDimensionsFieldConfigs.get(fieldName);
        if (fieldVersionsMap == null) {
            fieldVersionsMap = new HashMap<String,FieldConfig>();
            configuredCustomDimensionsFieldConfigs.put(fieldName, fieldVersionsMap);
        }

        fieldVersionsMap.put(version, fieldConfig);
    }

    private void addFieldToEventFieldsMap(String eventName, String fieldName, String version, FieldConfig fieldConfig) {

        HashMap<String,HashMap<String,FieldConfig>> eventFiledsMap = configuredEventsFieldConfigs.get(eventName);
        if (eventFiledsMap == null) {
            eventFiledsMap = new HashMap<String,HashMap<String,FieldConfig>>();
            configuredEventsFieldConfigs.put(eventName, eventFiledsMap);
        }
        HashMap<String,FieldConfig> fieldVersionsMap = eventFiledsMap.get(fieldName);
        if (fieldVersionsMap == null) {
            fieldVersionsMap = new HashMap<String,FieldConfig>();
            eventFiledsMap.put(fieldName, fieldVersionsMap);
        }

        fieldVersionsMap.put(version, fieldConfig);
    }

    public FieldConfig getFieldConfigByFullName(String fieldName) {
        Set<String> commonFieldNames = configuredCommonFieldConfigs.keySet();
        for (String commonFieldName : commonFieldNames) {
            Set<String> versions = configuredCommonFieldConfigs.get(commonFieldName).keySet();
            for (String version : versions) {
                FieldConfig config = configuredCommonFieldConfigs.get(commonFieldName).get(version);
                String fieldParquetName = config.getParquetField();
                if(fieldParquetName==null ||fieldParquetName.isEmpty()) {
                    fieldParquetName = commonFieldName;
                }
                if (fieldName.equals(fieldParquetName)) {
                    return config;
                }
            }
        }

        Set<String> eventNames = configuredEventsFieldConfigs.keySet();
        for (String eventName : eventNames) {
            Set<String> eventFieldNames = configuredEventsFieldConfigs.get(eventName).keySet();
            for (String eventFieldName : eventFieldNames) {
                Set<String> versions = configuredEventsFieldConfigs.get(eventName).get(eventFieldName).keySet();
                for (String version : versions) {
                    FieldConfig config = configuredEventsFieldConfigs.get(eventName).get(eventFieldName).get(version);
                    String fieldParquetName = config.getParquetField();
                    if(fieldParquetName==null ||fieldParquetName.isEmpty()) {
                        fieldParquetName = eventName + "." + eventFieldName;
                    }
                    if (fieldName.equals(fieldParquetName)) {
                        return config;
                    }
                }
            }
        }

        Set<String> customDimensionsFieldNames = configuredCustomDimensionsFieldConfigs.keySet();
        for (String customDimensionsFieldName : customDimensionsFieldNames) {
            Set<String> versions = configuredCustomDimensionsFieldConfigs.get(customDimensionsFieldName).keySet();
            for (String version : versions) {
                FieldConfig config = configuredCustomDimensionsFieldConfigs.get(customDimensionsFieldName).get(version);
                String fieldParquetName = config.getParquetField();
                if(fieldParquetName==null ||fieldParquetName.isEmpty()) {
                    fieldParquetName = customDimensionsFieldName;
                }
                if (fieldName.equals(fieldParquetName)) {
                    return config;
                }
            }
        }
        return null;
    }

    public static String getEventSchemaVersion ( JsonNode event) {
        if (event.get(SCHEMA_VERSION_FIELD_NAME)!=null && event.get(SCHEMA_VERSION_FIELD_NAME).isTextual()) {
            return event.get(SCHEMA_VERSION_FIELD_NAME).asText();
        }
        return null;
    }
}
