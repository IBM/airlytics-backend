package com.ibm.airlytics.consumer.userdb;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class StatementWithLineage {
    private PreparedStatement statement;
    private String userId;
    private EventLineage lineage;

    public StatementWithLineage(PreparedStatement statement, String userId, EventLineage lineage) {
        this.userId = userId;
        this.statement = statement;
        this.lineage = lineage;
    }

    public PreparedStatement getStatement() { return statement; }
    public EventLineage getLineage() { return lineage; }
}
