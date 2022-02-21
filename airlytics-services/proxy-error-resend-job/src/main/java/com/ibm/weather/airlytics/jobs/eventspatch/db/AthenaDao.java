package com.ibm.weather.airlytics.jobs.eventspatch.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.weather.airlytics.common.athena.AthenaBasicDao;
import com.ibm.weather.airlytics.common.athena.AthenaDriver;
import com.ibm.weather.airlytics.common.dto.AirlyticsEvent;
import com.ibm.weather.airlytics.common.dto.BasicAthenaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;

import java.util.List;
import java.util.function.Consumer;

@Repository
public class AthenaDao extends AthenaBasicDao {

    private static final Logger logger = LoggerFactory.getLogger(AthenaDao.class);

    final private ObjectMapper mapper = new ObjectMapper();

    @Autowired
    public AthenaDao(AthenaDriver athena) {
        super(athena);
    }

    public void processEvents(
            AthenaClient athenaClient,
            BasicAthenaConfig featureConfig,
            final String query,
            final Consumer<AirlyticsEvent> consumer)
            throws Exception {

        executeQueryProcessResult(
                athenaClient,
                featureConfig,
                query,
                (rows, meta) -> {

                    if(rows.size() > 0) {//0 is header
                        boolean header = true;

                        for (Row row : rows) {

                            if(header) {
                                header = false;
                                continue;
                            }
                            List<Datum> cols = row.data();

                            if (!cols.isEmpty()) {
                                Datum eventCol = cols.get(0);
                                String eventJson = eventCol.varCharValue();

                                try {
                                    AirlyticsEvent event = mapper.readValue(eventJson, AirlyticsEvent.class);
                                    consumer.accept(event);
                                } catch (Exception e) {
                                    logger.error("Error parsing column 0: {}", eventJson);
                                }
                            }
                        }
                    }

                });
    }

}
