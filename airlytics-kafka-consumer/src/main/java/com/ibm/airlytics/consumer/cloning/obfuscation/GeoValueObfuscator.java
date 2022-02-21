package com.ibm.airlytics.consumer.cloning.obfuscation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

public class GeoValueObfuscator extends AbstractFieldValueObfuscator {

    @Override
    protected boolean obfuscateValueNode(ObjectNode parentNode, String fieldName, JsonNode valueNode) {

        if(valueNode instanceof DoubleNode) {
            double value = valueNode.doubleValue();
            StringBuilder sb = new StringBuilder(String.valueOf((int) (double) value));// truncated to the int part
            sb.append('.');
            IntStream.range(1, 7)
                    .boxed()
                    .forEach(i -> sb.append(ThreadLocalRandom.current().nextInt(0, 10)));
            parentNode.put(fieldName, Double.valueOf(sb.toString()));
            return true;
        }
        return false;
    }
}
