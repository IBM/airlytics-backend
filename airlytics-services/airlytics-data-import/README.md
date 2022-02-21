# Airlytics Data Import Service
A service to import bulk data into tables in Airlytics Users DB.

## Introduction

### Additional User Features Intro

Airlytics Users DB may contain a set of tables to store additional users’ features, for example features calculated using 
predictive analytics based on Airlytics events history. Airlytics Data Import Service allows for importing these features
into Airlytics Users DB for further use (e.g. for dashboards, or cohort calculation).

### Additional User Features Requirements

While specifying AI feature tables and columns, one must follow certain conventions. This is important, since Airlytics 
services’ software relies on these conventions in order to recognize and manage the data structures.
1. Every AI feature table name must start with the following prefix: “user_features_”.
2. Every AI feature table must contain the following columns in addition to the feature column:
```sql
user_id varchar(64) NOT NULL,
shard int4 NOT NULL,
created_at timestamp NOT NULL DEFAULT now()
```
3. user_id column in every feature table must be its primary key.
4. The Airlytics Users DB product schema having user features has to have a view called “user_features_all” containing all 
features from all feature tables. This view definition is updated automatically and the view is re-created by 
Airlytics Data Import Service, with each data import.
5. It is recommended that all AI feature tables should be owned by a special DB user “airlytics_data_import”.

### Data Import Requirements

User features data may be imported from CSV files on AWS S3 into Airlytics Users DB with Airlytics Data Import Service 
running on Airlock platform using its API, or Airlock GUI panel. To use the API, or the panel, one needs credentials for 
Airlock.

To add a source S3 bucket that can be used for Data Import, one should follow instruction on setting up RDS access to 
that bucket, as described in [Amazon AWS Documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_PostgreSQL.S3Import.html#USER_PostgreSQL.S3Import.AccessPermission).

Important Note: For performance reasons, data import is not incremental, and each imported CSV must contain data for 
**all** users, and the data for **all** columns for each user in the target AI feature table.

A CSV containing the complete data for a certain user features table should comply with the following rules:
1. The CSV must have a header with column names that exactly match the column names in the target User Features table 
in the Airlytics database
2. The first column must always be user_id, the second - shard
3. created_at column must not be present, its value is auto-generated
4. For null values, there should be no value (no space between separating commas, no quotes)
5. Column names in the header, as well as all text and array values should be surrounded by quotes
6. Array values should be surrounded by curly braces {} (within the quotes). Array items should be separated by commas
7. The file can be gzip-ed, in this case it must have either '.gz', or '.gzip' extension, the file must contain the following Amazon S3 metadata: Content-Encoding:gzip
8. JSON values are also surrounded with quotes, their inner quotes are escaped by doubling them

## Key Classes

The service provides RESTful API for CRUD operations on import job. See
**com.ibm.weather.airlytics.dataimport.api.DataImportController** Swagger-annotated Spring controller.

The service relies on Airlock BE Data Import API to obtain cohort definitions, and report back import job 
status. See **com.ibm.weather.airlytics.dataimport.integrations.AirlockDataImportClient** class implementing all these API calls.

## Service Configuration

The service requires setting a number of environment variables. Here is an example of environment configuration for a pod in Kubernetes:
```yaml
      containers:
        - name: ai-data-import
          image: my.imagestore.com/airlytics/ai-data-import:latest
          ports:
            - containerPort: 8081
              name: web
              protocol: TCP
          env:
            - name: AIRLYTICS_ENVIRONMENT
              value: 'PROD'
            - name: AIRLYTICS_PRODUCT
              value: 'Hello World App'
            - name: AIRLYTICS_DEPLOYMENT
              valueFrom:
                configMapKeyRef:
                  name: airlytics-deployments-config
                  key: airlytics.deployment
            - name: DB_URL
              value: 'jdbc:postgresql://airlytics-userdb-prod.mydbserver.com:5432/users?currentSchema=hello_world'
            - name: DB_USER_NAME
              value: 'airlytics_data_import'
            - name: DB_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: userdb-secret
                  key: airlytics_data_import
            - name: AIRLOCK_KEY
              valueFrom:
                secretKeyRef:
                  name: airlock-api-secret
                  key: airlock-key
            - name: AIRLOCK_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: airlock-api-secret
                  key: airlock-password
```

Airlock configuration for the service may look like
```json
{
  "tempBucketFolderPath": "airlytics-imports-internal",
  "productId": "12345",
  "dbAwsRegion": "eu-west-1",
  "resumeInterruptedJobs": "true",
  "airlockApiBaseUrl": "http://arlock-adminapi.myairlock.com:80/airlock/api",
  "athenaImportTables": [
    "ad_impressions"
  ],
  "featureTables": [
    "user_features_retention",
    "user_features_session"
  ],
  "piFeatureTables": [
    "user_features_test_pi"
  ]
}
```
Note that athenaImportTables property in the example above is used to import aggregated data generated by Airlytics 
Event Aggregator cronjob service (see **event-aggregator-job** service module) in Athena.