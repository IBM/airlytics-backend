package com.ibm.airlytics.consumer.cloning.obfuscation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.airlytics.consumer.cloning.config.CloningConsumerConfig;
import com.ibm.airlytics.consumer.cloning.config.EventFieldsObfuscationConfig;
import com.ibm.airlytics.consumer.cloning.config.ObfuscationConfig;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Obfuscator {

    private static final String COMMON_RULES_NAME = "*";

    private static final String FIELD_EVENT_NAME = "name";

    private static final String JSONPATH_ROOT = "$.";

    // Configure JsonPath for Jackson JSON
    static {
        Configuration.setDefaults(new Configuration.Defaults() {

            private final JsonProvider jsonProvider = new JacksonJsonNodeJsonProvider();
            private final MappingProvider mappingProvider = new JacksonMappingProvider();

            @Override
            public JsonProvider jsonProvider() {
                return jsonProvider;
            }

            @Override
            public MappingProvider mappingProvider() {
                return mappingProvider;
            }

            @Override
            public Set<Option> options() {
                return EnumSet.noneOf(Option.class);
            }
        });
    }

    private GuidHashValueObfuscator guidHashValueObfuscator;

    private GuidRandomValueObfuscator guidRandomValueObfuscator;

    private GeoValueObfuscator geoValueObfuscator;

    private Map<String, EventFieldsObfuscationConfig> eventSpecificRules;

    public Obfuscator(CloningConsumerConfig config) {
        this((config != null && config.getEventProxyIntegrationConfig() != null) ? config.getEventProxyIntegrationConfig().getObfuscation() : null);
    }

    public Obfuscator(ObfuscationConfig config) {

        if(config != null) {
            this.guidHashValueObfuscator = new GuidHashValueObfuscator(config.getHashPrefix());
            this.guidRandomValueObfuscator = new GuidRandomValueObfuscator();
            this.geoValueObfuscator = new GeoValueObfuscator();

            if (config.getEventRules() != null) {
                this.eventSpecificRules =
                        config.getEventRules()
                                .stream()
                                .collect(
                                        Collectors.toMap(
                                                EventFieldsObfuscationConfig::getEvent,
                                                Function.identity()));
            }
        }
    }

    public boolean hasRules() {
        return MapUtils.isNotEmpty(this.eventSpecificRules);
    }

    public void obfuscate(JsonNode event) {
        // apply common rules
        applyRules(event, this.eventSpecificRules.get(COMMON_RULES_NAME));
        // apply rules defined only for this event's type
        applyRules(event, this.eventSpecificRules.get(event.get(FIELD_EVENT_NAME).textValue()));
    }

    private void applyRules(JsonNode event, EventFieldsObfuscationConfig eventRules) {

        if(eventRules != null) {
            processFields(event, eventRules.getGuidHash(), this.guidHashValueObfuscator);
            processFields(event, eventRules.getGuidRandom(), this.guidRandomValueObfuscator);
            processFields(event, eventRules.getGeo(), this.geoValueObfuscator);
        }
    }

    private void processFields(JsonNode event, List<String> paths, AbstractFieldValueObfuscator processor) {

        if(CollectionUtils.isNotEmpty(paths)) {
            paths.forEach(path -> processField(event, path, processor));
        }

    }

    private void processField(JsonNode json, String path, AbstractFieldValueObfuscator processor) {

        if(StringUtils.isNotBlank(path)) {

            if (path.indexOf('.') > 0) {
                String pathToParentObject = path.substring(0, path.lastIndexOf('.'));
                String fieldName = path.substring(path.lastIndexOf('.') + 1);

                if(!pathToParentObject.startsWith(JSONPATH_ROOT)) {
                    pathToParentObject = JSONPATH_ROOT + pathToParentObject;
                }

                try {
                    Object parentObject = JsonPath.read(json, pathToParentObject);

                    if (parentObject instanceof ObjectNode) {
                        processField((ObjectNode) parentObject, fieldName, processor);
                    } else if (parentObject instanceof ArrayNode) {
                        ArrayNode nodes = (ArrayNode) parentObject;
                        nodes.elements().forEachRemaining(node -> processField(node, fieldName, processor));
                    }
                } catch(PathNotFoundException e) {
                    // the given event does not have this property
                }
            } else {
                processor.obfuscateValueNode((ObjectNode) json, path);
            }
        }
    }
}
