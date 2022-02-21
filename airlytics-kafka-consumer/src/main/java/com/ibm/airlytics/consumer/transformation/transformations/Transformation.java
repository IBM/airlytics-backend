package com.ibm.airlytics.consumer.transformation.transformations;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.consumer.transformation.TransformedUserEvent;

import java.util.Optional;

public abstract class Transformation {

    public static String transformationId(){
        return "";
    }

    public abstract Optional<JsonNode> transform(TransformedUserEvent originalEvent, ObjectMapper mapper);
}
