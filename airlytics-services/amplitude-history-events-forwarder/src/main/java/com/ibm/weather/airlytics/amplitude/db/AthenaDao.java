package com.ibm.weather.airlytics.amplitude.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.weather.airlytics.amplitude.dto.AirlyticsEvent;
import com.ibm.weather.airlytics.amplitude.dto.AmplitudeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;

import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Repository
public class AthenaDao {

    private static final Logger logger = LoggerFactory.getLogger(AthenaDao.class);

    final private ObjectMapper mapper = new ObjectMapper();

    @Value("${athena.rawevents.table:}")
    protected String athenaRawevents;

    private AthenaDriver athena;

    @Autowired
    public AthenaDao(AthenaDriver athena) {
        this.athena = athena;
    }

    public void processEvents(
            AthenaClient athenaClient,
            final LocalDate day,
            final long endTime,
            final String maxAppVersion,
            final List<String> eventNames,
            final Consumer<AirlyticsEvent> consumer)
            throws Exception {
        String query = buildProcessEventsQuery(day, endTime, maxAppVersion, eventNames);
        final AtomicInteger cnt = new AtomicInteger(0);
        executeQueryProcessResult(
                athenaClient,
                query,
                (rows, meta) -> {

                    if(rows.size() > 0) {//0 is header
                        for (Row row : rows) {
                            List<Datum> cols = row.data();

                            if (!cols.isEmpty()) {
                                Datum eventCol = cols.get(0);
                                String eventJson = eventCol.varCharValue();

                                if(!"event".equalsIgnoreCase(eventJson)) {
                                    try {
                                        AirlyticsEvent event = mapper.readValue(eventJson, AirlyticsEvent.class);
                                        consumer.accept(event);
                                    } catch (Exception e) {
                                        logger.error("Error parsing event {}", eventJson);
                                    }
                                }
                            }
                        }
                    }

                });
    }

    private String buildProcessEventsQuery(
            final LocalDate day,
            final long endTime,
            final String maxAppVersion,
            final List<String> eventNames) {

        StringBuilder sb = new StringBuilder();

        sb.append("select event from ")
                .append(this.athenaRawevents)
                .append(" where day = '").append(day).append("'")
                .append(" and recievedtimestamp < ").append(endTime)
                .append(" and appversion != '").append(maxAppVersion).append("'")
                .append("and name in (");

        for(int i = 0; i < eventNames.size(); i++) {
            sb.append("'").append(eventNames.get(i)).append("'");

            if(i < (eventNames.size() - 1)) {
                sb.append(",");
            }
        }
        sb.append(")");

        return sb.toString();
    }

    private void executeQueryProcessResult(
            AthenaClient athenaClient,
            String query,
            BiConsumer<List<Row>, List<ColumnInfo>> rowConsumer) throws Exception {
        logger.info("Executing {}", query);
        String queryExecutionId = athena.submitAthenaQuery(query, athenaClient);
        logger.info("Got Execution ID {}", queryExecutionId);
        QueryExecutionState result = athena.waitForQueryToComplete(queryExecutionId, athenaClient);
        logger.info("Status for Execution ID {}: {}", queryExecutionId, result);

        switch (result) {
            case FAILED:
            case CANCELLED:
                throw new Exception("Athena query " + queryExecutionId + " " + result.toString() + ": " + query);
        }
        athena.processResultRows(queryExecutionId, athenaClient, rowConsumer);
    }
}
