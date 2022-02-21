package com.ibm.analytics.queryservice.airlock;


/**
 * Automatically generated file by Airlock server. DO NOT MODIFY
 */
@SuppressWarnings("ALL")
public class AirlockConstants {
    public static class DSR {
        public static final String DSR_CONFIG="DSR.DSR Config";
        public static final String DSR_CURSOR_CONFIG="dsr.DSR Cursor Config";
        public static final String DSR_SCRAPPER="dsr.DSR Scrapper";
    }
    public static class infrastracture {
        public static final String RABBITMQ_CONFIGURATION="infrastracture.RabbitMQ Configuration";
        public static final String SECRETS="infrastracture.Secrets";
        public static final String PRODUCT_IDS="infrastracture.Product Ids";
        public static final String EVENTS_PROXY_CONFIGURATION="infrastracture.Events Proxy Configuration";
        public static final String KAFKA_CONFIGURATIONS="infrastracture.Kafka Configurations";
        public static final String PUSH_HANDLER_CONFIGURATIONS="infrastracture.Push Handler Configurations";
        public static final String DB_CONNECTION_CONFIGURATIONS="infrastracture.DB connection configurations";
        public static final String TRACKER_TIMING_CONFIGURATIONS="infrastracture.Tracker Timing Configurations";
        public static final String CONFIGURATIONS="infrastracture.Configurations";
    }
    public static class data {
        public static final String DATA_PORTABILITY="data.Data Portability";
    }
    public static class query {
        public static final String DB_PRUNER="query.DB Pruner";
        public static final String REMOVE_DEV_USERS="query.Remove dev users";
        public static final String DB_DUMPER="query.DB Dumper";
        public static final String INACTIVE_USERS_QUERY="query.Inactive Users Query";
    }
    public static class Consumers {
        public static final String ANALYTICS_CONSUMER="Consumers.Analytics Consumer";
        public static final String PERSISTENCE_CONSUMER="Consumers.Persistence Consumer";
        public static final String RETRIEVER_CONFIG="Consumers.Retriever Config";
        public static final String LTV_INPUT_CONSUMER="Consumers.LTV Input Consumer";
    }
    public static class delete {
        public static final String DELETE_USER="delete.Delete User";
    }
    public static class retention {
        public static final String GET_USER_BY_ID="retention.Get user by id";
        public static final String SET_USER_INACTIVE="retention.Set user inactive";
        public static final String GET_INACTIVE_USERS="retention.Get inactive users";
        public static final String SET_USERS_IDS_INACTIVE="retention.Set users ids inactive";
        public static final String RETENTION_TRACKER_DB_QUERIES="retention.Retention Tracker DB queries";
    }
}

