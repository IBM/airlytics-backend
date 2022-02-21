package com.ibm.airlytics.consumer.cloning.obfuscation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.lang3.StringUtils;

import java.util.UUID;

public class GuidHashValueObfuscator extends AbstractFieldValueObfuscator {

    private String prefix;

    public GuidHashValueObfuscator(String prefix) {
        super();
        this.prefix = prefix;
    }

    @Override
    protected boolean obfuscateValueNode(ObjectNode parentNode, String fieldName, JsonNode valueNode) {

        if(valueNode instanceof TextNode) {
            String value = valueNode.textValue();
            if (StringUtils.isNotBlank(value)) {
                String obf = UUID.nameUUIDFromBytes(value.getBytes()).toString();

                if (StringUtils.isNotBlank(prefix)) {// replace bytes at the start
                    obf = prefix + obf.substring(prefix.length());
                }
                parentNode.put(fieldName, obf);
                return true;
            }
        }
        return false;
    }
}
