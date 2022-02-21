package com.ibm.airlytics.consumer.cloning.obfuscation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.lang3.StringUtils;

import java.util.UUID;

public class GuidRandomValueObfuscator extends AbstractFieldValueObfuscator {

    @Override
    protected boolean obfuscateValueNode(ObjectNode parentNode, String fieldName, JsonNode valueNode) {

        if(valueNode instanceof TextNode) {
            String value = valueNode.textValue();

            if (StringUtils.isNotBlank(value)) {
                parentNode.put(fieldName, UUID.randomUUID().toString());
                return true;
            }
        }
        return false;
    }
}
