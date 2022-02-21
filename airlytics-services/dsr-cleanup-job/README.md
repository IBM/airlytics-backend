# Airlytics DSR Cleanup Job

## Introduction

A service to perform cleanup for users deleted by DSR process from DB dumps on S3.

## Key Classes

**com.ibm.weather.airlytics.jobs.dsr.services.JobExecutionService** automatically starts the job shortly after
the apllication starts, and exit, when finished. The process that starts the application periodically (e.g. once a day)
on a certain schedule can be implemented, as a Kubernetes CronJob.

## Service Configuration

The service requires setting a number of environment variables. Here is an example of environment configuration for a pod in Kubernetes:
```yaml
apiVersion: batch/v1beta1
kind: CronJob
metadata:
  name: dsr-cleanup-helloworld-prod
  namespace: airlytics
  labels:
    app: dsr-cleanup-helloworld
  annotations:
    deployment.kubernetes.io/revision: '3'
spec:
  schedule: "30 6 * * 0,3"
  successfulJobsHistoryLimit: 1
  failedJobsHistoryLimit: 1
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      backoffLimit: 0
      template:
        metadata:
          labels:
            app: dsr-cleanup-helloworld
        spec:
          volumes:
            - name: persistence-pv-storage
              persistentVolumeClaim:
                claimName: helloworld-aws-efs
          containers:
            - name: dsr-cleanup
              image: my.imagestore.com/airlytics/dsr-cleanup-job:latest
              volumeMounts:
                - name: persistence-pv-storage
                  mountPath: /usr/src/app/data
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
  "tempFolder": "dsr-temp/HelloWorld/",
  "targetS3Bucket": "airlytics-datalake-internal-prod-pi",
  "athenaRegion": "eu-west-1",
  "athenaResultsOutputBucket": "aws-athena-query-results",
  "archivedPartAfter": "HelloWorld/",
  "athenaCatalog": "AwsDataCatalog",
  "markersBaseFolder": "/usr/src/app/data/airlytics-datalake-internal-prod/userDeletionRequests/DbDumps/HelloWorld/",
  "markersArchiveFolder": "/usr/src/app/data/airlytics-datalake-internal-prod/userDeletionRequests/DbDumps/processed/HelloWorld/",
  "cleanupTables": [
    {
      "piTable": "airlytics_users_db_helloworld_prod_pi",
      "userIdColumn": "id",
      "sourceS3Folder": "usersdb/users_pi/HelloWorld/",
      "sourceS3Bucket": "airlytics-datalake-internal-prod-pi",
      "athenaDb": "airlytics"
    },
    {
      "piTable": "airlytics_users_db_helloworld_prod_cohorts",
      "userIdColumn": "user_id",
      "sourceS3Folder": "usersdb/user_cohorts/HelloWorld/",
      "sourceS3Bucket": "airlytics-datalake-internal-prod-pi",
      "athenaDb": "airlytics"
    }
  ]
}
```