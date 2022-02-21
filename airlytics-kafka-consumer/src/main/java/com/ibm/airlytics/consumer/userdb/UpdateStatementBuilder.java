package com.ibm.airlytics.consumer.userdb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class UpdateStatementBuilder {
    private class FieldUpdate {
        private String type;
        private JsonNode value;

        public FieldUpdate(String type, JsonNode value) {
            this.type = type;
            this.value = value;
        }
    }

    private Map<String, FieldUpdate> fields = new LinkedHashMap<>();
    protected static ObjectMapper objectMapper = new ObjectMapper();

    public void addField(String name, String type, JsonNode value) {
        fields.put(name, new FieldUpdate(type, value));
    }

    public PreparedStatement prepareStatement(String tableName, String userId, Connection conn)
            throws SQLException, UpdateStatementBuilderException {
        if (fields.size() == 0)
            return null;

        String statementSql = fields.keySet().stream().map(fieldName -> fieldName + " = ?").collect(
                Collectors.joining(",", "UPDATE " + tableName + " SET ", " WHERE id = ?"));

        PreparedStatement statement = conn.prepareStatement(statementSql);

        int i = 1;

        for (Map.Entry<String, FieldUpdate> field : fields.entrySet()) {
            FieldUpdate fieldUpdate = field.getValue();

            switch (fieldUpdate.type) {
                case "string":
                    if (fieldUpdate.value.isNull())
                        statement.setNull(i, Types.VARCHAR);
                    else {
                        String stringValue = removeNullCharacters(fieldUpdate.value.asText());
                        statement.setString(i, stringValue);
                    }
                    break;
                case "integer":
                    if (fieldUpdate.value.isNull())
                        statement.setNull(i, Types.INTEGER);
                    else
                        statement.setInt(i, fieldUpdate.value.asInt());
                    break;
                case "boolean":
                    if (fieldUpdate.value.isNull())
                        statement.setNull(i, Types.BOOLEAN);
                    else
                        statement.setBoolean(i, fieldUpdate.value.asBoolean());
                    break;
                case "timestamp":
                    if (fieldUpdate.value.isNull())
                        statement.setNull(i, Types.TIMESTAMP);
                    else
                        statement.setTimestamp(i, createTimestamp(fieldUpdate.value.asLong(), field));
                    break;
                case "double":
                    if (fieldUpdate.value.isNull())
                        statement.setNull(i, Types.DOUBLE);
                    else
                        statement.setDouble(i, fieldUpdate.value.asDouble());
                    break;
                case "long":
                    if (fieldUpdate.value.isNull())
                        statement.setNull(i, Types.BIGINT);
                    else
                        statement.setLong(i, fieldUpdate.value.asLong());
                    break;
                case "json":
                    if (fieldUpdate.value.isNull())
                        statement.setNull(i, Types.OTHER);
                    else {
                        String strJson;
                        try {
                            strJson = objectMapper.writeValueAsString(fieldUpdate.value);
                        } catch (JsonProcessingException e) {
                            throw new UpdateStatementBuilderException("Cannot convert json to string in field: " + field.getKey(), e);
                        }
                        statement.setObject(i, strJson, Types.OTHER);
                    }
                    break;
                default:
                    if (fieldUpdate.type.startsWith("array of ")) {
                        if (fieldUpdate.value.isNull())
                            statement.setNull(i, Types.ARRAY);
                        else {
                            Array array = extractArray(field, conn, fieldUpdate.type.substring(9).trim());
                            statement.setArray(i, array);
                        }
                    }
                    else {
                        throw new UpdateStatementBuilderException("Unrecognized type defined for dynamic field: " + fieldUpdate.type);
                    }
                    break;
            }
            ++i;
        }

        statement.setString(i, userId);
        return statement;
    }

    private String removeNullCharacters(String stringValue) {
        if (stringValue.indexOf('\0') > -1)
            stringValue = stringValue.replace("\0", "");

        return stringValue;
    }

    private Array extractArray(Map.Entry<String, FieldUpdate> field, Connection conn, String elementType)
            throws SQLException, UpdateStatementBuilderException {
        FieldUpdate fieldUpdate = field.getValue();

        if (!fieldUpdate.value.isArray())
            throw new UpdateStatementBuilderException("Array type specified but json node is not an array. Field: "
                    + field.getKey());

        Object[] items = null;
        String sqlType = null;

        switch (elementType) {
            case "string":
                sqlType = JDBCType.VARCHAR.getName();
                items = new String[fieldUpdate.value.size()];
                for (int n = 0; n < fieldUpdate.value.size(); ++n)
                    items[n] = removeNullCharacters(fieldUpdate.value.get(n).asText());
                break;
            case "integer":
                sqlType = JDBCType.INTEGER.getName();
                items = new Integer[fieldUpdate.value.size()];
                for (int n = 0; n < fieldUpdate.value.size(); ++n)
                    items[n] = fieldUpdate.value.get(n).asInt();
                break;
            case "boolean":
                sqlType = JDBCType.BOOLEAN.getName();
                items = new Boolean[fieldUpdate.value.size()];
                for (int n = 0; n < fieldUpdate.value.size(); ++n)
                    items[n] = fieldUpdate.value.get(n).asBoolean();
                break;
            case "timestamp":
                sqlType = JDBCType.TIMESTAMP.getName();
                items = new Timestamp[fieldUpdate.value.size()];
                for (int n = 0; n < fieldUpdate.value.size(); ++n)
                    items[n] = createTimestamp(fieldUpdate.value.get(n).asLong(), field);
                break;
            case "double":
                sqlType = JDBCType.FLOAT.getName();
                items = new Double[fieldUpdate.value.size()];
                for (int n = 0; n < fieldUpdate.value.size(); ++n)
                    items[n] = fieldUpdate.value.get(n).asDouble();
                break;
            case "long":
                sqlType = JDBCType.BIGINT.getName();
                items = new Long[fieldUpdate.value.size()];
                for (int n = 0; n < fieldUpdate.value.size(); ++n)
                    items[n] = fieldUpdate.value.get(n).asLong();
                break;
            default:
                throw new UpdateStatementBuilderException("Unrecognized type defined for dynamic field array: " + elementType);
        }

        if (sqlType == null || items == null)
            throw new UpdateStatementBuilderException("Could not create dynamic field array of type: " + elementType);

        return conn.createArrayOf(sqlType, items);
    }

    private Timestamp createTimestamp(long longValue, Map.Entry<String, FieldUpdate> field) throws UpdateStatementBuilderException {
        // Validate it won't break the DB
        // Mon 03 Sep 68 01:20:00 PM UTC - Sat 29 Apr 3871 10:40:00 AM UTC
        if (longValue < -60000000000000l || longValue > 60000000000000l)
            throw new UpdateStatementBuilderException("Timestamp out of bounds for field: " + field.getKey());

        return new Timestamp(longValue);
    }
}