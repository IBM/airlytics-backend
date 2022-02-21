# Airlytics Event Aggregator CronJob

## Introduction

A service to perform periodical aggregations for raw Airlytics events in Athena. One example is user LTV aggregation.

## Key Classes

**com.ibm.weather.airlytics.jobs.aggregate.services.JobExecutionService** automatically starts the job shortly after 
the apllication starts, and exit, when finished. The process that starts the application periodically (e.g. once a day)
on a certain schedule can be implemented, as a Kubernetes CronJob. 

## Service Configuration

The service requires setting a number of environment variables. Here is an example of environment configuration for a pod in Kubernetes:
```yaml
apiVersion: batch/v1beta1
kind: CronJob
metadata:
  name: ltv-aggregator-helloworld-prod
  namespace: airlytics
  labels:
    app: ltv-aggregator-helloworld
  annotations:
    deployment.kubernetes.io/revision: '3'
spec:
  schedule: "0 3 * * *"
  successfulJobsHistoryLimit: 1
  failedJobsHistoryLimit: 1
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      backoffLimit: 0
      template:
        metadata:
          labels:
            app: ltv-aggregator-helloworld
        spec:
          containers:
            - name: event-aggregator
              image: my.imagestore.com/airlytics/event-aggregator-job:latest
              ports:
                - containerPort: 8081
                  name: web
                  protocol: TCP
              env:
                - name: AIRLYTICS_AGGREGATION
                  value: 'LTV'
                - name: AIRLYTICS_ENVIRONMENT
                  value: 'PROD'
                - name: AIRLYTICS_PRODUCT
                  value: 'Hello World App'
                - name: AIRLYTICS_DEPLOYMENT
                  valueFrom:
                    configMapKeyRef:
                      name: airlytics-deployments-config
                      key: airlytics.deployment
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
              livenessProbe:
                httpGet:
                  path: /healthcheck/is-alive
                  port: web
                initialDelaySeconds: 30
                periodSeconds: 30
              readinessProbe:
                httpGet:
                  path: /healthcheck/is-ready
                  port: web
                initialDelaySeconds: 30
                periodSeconds: 30
              resources:
                limits:
                  memory: 256Mi
                requests:
                  memory: 256Mi
              terminationMessagePath: /dev/termination-log
              terminationMessagePolicy: File
              imagePullPolicy: Always
          restartPolicy: Never
          terminationGracePeriodSeconds: 30
          dnsPolicy: ClusterFirst
          securityContext: {}
          schedulerName: default-scheduler
```

Airlock configuration for the service may look like
```json
{
  "targetS3Bucket": "airlytics-datalake-internal-prod",
  "athenaRegion": "eu-west-1",
  "sourceAthenaDb": "airlytics",
  "importToUserDb": true,
  "historyStartDay": "2021-06-01",
  "sourceEventsTable": "airlytics_rawevents_helloworld_prod",
  "airlockProductId": "12345",
  "athenaCatalog": "AwsDataCatalog",
  "dailyAggregationsTable": "airlytics_aggregations_ltv_helloworld_prod_1d",
  "exportToCsv": true,
  "aggregatedValueJsonPath": "$.attributes.revenue",
  "athenaResultsOutputBucket": "aws-athena-query-results-eu-west-1",
  "targetColumn": "revenue",
  "importTargetTable": "ad_impressions",
  "dailyAggregationsS3Path": "/aggregations/ltv/HelloWorld/1d/",
  "csvTablePrefix": "ad_impressions_helloworld_prod",
  "eventName": "ad-impression",
  "destAthenaDb": "aggregations_helloworld",
  "csvOutputBucket": "airlytics-imports-internal",
  "csvOutputFolder": "/db-import/aggregations/ltv/HelloWorld/",
  "secondaryAggregations": [
    {
      "targetFolder": "/aggregations/ltv/HelloWorld/7d/",
      "targetTable": "airlytics_aggregations_ltv_helloworld_prod_7d",
      "aggregationWindowDays": 7
    },
    {
      "targetFolder": "/aggregations/ltv/HelloWorld/30d/",
      "targetTable": "airlytics_aggregations_ltv_helloworld_prod_30d",
      "aggregationWindowDays": 30
    },
    {
      "targetFolder": "/aggregations/ltv/HelloWorld/365d/",
      "targetTable": "airlytics_aggregations_ltv_helloworld_prod_365d",
      "aggregationWindowDays": 365
    },
    {
      "targetFolder": "/aggregations/ltv/HelloWorld/total/",
      "targetTable": "airlytics_aggregations_ltv_helloworld_prod_total",
      "aggregationWindowDays": 36500
    }
  ],
  "aggregatedValueType": "bigint",
  "checkDataForLastDay": true,
  "airlockApiBaseUrl": "http://arlock-adminapi.myairlock.com:80/airlock/api"
}
```

