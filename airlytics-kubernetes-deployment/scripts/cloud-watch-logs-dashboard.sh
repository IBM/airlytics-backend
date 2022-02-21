#!/bin/bash

#load cluser config
source ./cluster.properties

# login into cluster
chmod +x login_cluster.sh
./login_cluster.sh


## declare services array
declare -a services=("event-proxy" "userdb-parquet-dumper" "retention-tracker-db-handler" "retention-tracker-push-handler" "db-pruner" "kafdrop" \
 "ai-data-import-product-prod" "air-cohorts-product-prod" "ai-data-import-product-prod" "air-cohorts-product-prod" \
 "ltv-aggregator-product-prod" "ltv-aggregator-product-prod" \
 "airlytics-ro-superspeed"  "dsr-requests-scrapper"  "air-cohorts-prod" "ai-data-import-prod"  "ltv-reader-prod" \
 "amplitude-history-upload-ios" "event-proxy-probe" "polls-product-prod")