package com.ibm.airlytics.retentiontracker.airlock;

/**
 * Automatically generated file by Airlock server. DO NOT MODIFY
 */
public class AirlockConstants {
    public class DSR {
        public static final String DSR_CONFIG="DSR.DSR Config";
        public static final String DSR_CURSOR_CONFIG="dsr.DSR Cursor Config";
        public static final String DSR_SCRAPPER="dsr.DSR Scrapper";
    }
    public class infrastracture {
        public static final String RABBITMQ_CONFIGURATION="infrastracture.RabbitMQ Configuration";
        public static final String SECRETS="infrastracture.Secrets";
        public static final String PRODUCT_IDS="infrastracture.Product Ids";
        public static final String EVENTS_PROXY_CONFIGURATION="infrastracture.Events Proxy Configuration";
        public static final String KAFKA_CONFIGURATIONS="infrastracture.Kafka Configurations";
        public static final String PUSH_HANDLER_CONFIGURATIONS="infrastracture.Push Handler Configurations";
        public static final String DB_CONNECTION_CONFIGURATIONS="infrastracture.DB connection configurations";
        public static final String TRACKER_TIMING_CONFIGURATIONS="infrastracture.Tracker Timing Configurations";
        public static final String ATHENA_HANDLER_CONFIGURATIONS="infrastracture.Athena Handler Configurations";
        public static final String RDS_DB_EXPORTER_CONFIGURATIONS="infrastracture.RDS DB Exporter Configurations";
        public static final String POLLS_CONFIGURATION="infrastracture.Polls Configuration";
        public static final String AIRLOCK_API_CONFIGURATIONS="infrastracture.Airlock API configurations";
        public static final String CONFIGURATIONS="infrastracture.Configurations";
    }
    public class data {
        public static final String DATA_PORTABILITY="data.Data Portability";
    }
    public class Test {
        public static final String SUB1="Test.sub1";
        public static final String TEST="Test.Test";
    }
    public class query {
        public static final String DB_PRUNER="query.DB Pruner";
        public static final String REMOVE_DEV_USERS="query.Remove dev users";
        public static final String ATHENA_QUERIES="query.Athena Queries";
        public static final String BOOKMARK_CONFIG="query.Bookmark Config";
        public static final String DB_DUMPER="query.DB Dumper";
        public static final String INACTIVE_USERS_QUERY="query.Inactive Users Query";
        public static final String POLLS="query.Polls";
    }
    public class Consumers {
        public static final String ANALYTICS_CONSUMER="Consumers.Analytics Consumer";
        public static final String PERSISTENCE_CONSUMER="Consumers.Persistence Consumer";
        public static final String SPLIT_CURSOR_CONFIG="Consumers.Split Cursor Config";
        public static final String PERSISTENCE_CONSUMER_SPLITTER="Consumers.Persistence Consumer Splitter";
        public static final String RETRIEVER_CONFIG="Consumers.Retriever Config";
    }
    public class airlockEntitlement {
        public static final String SUB="airlockEntitlement.sub";
        public static final String BASIC="airlockEntitlement.basic";
        public static final String PREMIUM="airlockEntitlement.premium";
        public static final String ENT="airlockEntitlement.ent";
        public static final String ENT2="airlockEntitlement.ent2";
    }
    public class delete {
        public static final String DELETE_USER="delete.Delete User";
    }
    public class retention {
        public static final String GET_USER_BY_ID="retention.Get user by id";
        public static final String SET_USER_INACTIVE="retention.Set user inactive";
        public static final String GET_INACTIVE_USERS="retention.Get inactive users";
        public static final String SET_USERS_IDS_INACTIVE="retention.Set users ids inactive";
        public static final String RETENTION_TRACKER_DB_QUERIES="retention.Retention Tracker DB queries";
    }
}
