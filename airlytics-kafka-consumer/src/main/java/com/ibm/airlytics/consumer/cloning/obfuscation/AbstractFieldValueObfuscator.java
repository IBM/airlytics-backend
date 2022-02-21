package com.ibm.airlytics.consumer.cloning.obfuscation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class AbstractFieldValueObfuscator {

    public boolean obfuscateValueNode(ObjectNode parentNode, String fieldName) {

        if(parentNode == null) {
            return false;
        }
        JsonNode valueNode = parentNode.get(fieldName);

        if(valueNode == null) {
            return false;
        }

        return obfuscateValueNode(parentNode, fieldName, valueNode);
    }

    protected abstract boolean obfuscateValueNode(ObjectNode parentNode, String fieldName, JsonNode valueNode);
}
