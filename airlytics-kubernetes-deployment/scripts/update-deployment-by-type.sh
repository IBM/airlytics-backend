#!/bin/bash
 # login cluster
chmod +x login_cluster.sh
./login_cluster.sh
source ./cluster.properties

echo "namespace "$NAMESPACE


## declare an array variable
declare -a consumers=( \
  "persistence-consumer" \
  "compaction-consumer"  \
  "userdb-consumer" \
  "raw-data" \
  "dsr-consumer" \
  "cloning" \
  "userdb-parquet-dumper" \
  "retention-tracker-db-handler" \
  "retention-tracker-push-handler" \
  "db-pruner" \
  "dsr-requests-scrapper" \
  "realtime-consumer" \
  "amplitude-transform-consumer" \
  "amplitude-forward-consumer" \
  "braze-transform-consumer" \
  "braze-forward-consumer" \
  "localytics-cohorts-consumer" \
  "ups-cohorts-consumer" \
  "braze-cohorts-consumer" \
  "amplitude-cohorts-consumer" \
  "braze-currents-consumer" \
  "purchase-consumer" \
  "transformation-consumer" \
  "ltv-processor-consumer" \
  "ai-data-import" \
  "air-cohorts" \
  "event-proxy" \
  "mparticle-transform-consumer" \
  "mparticle-forward-consumer" \
  "mparticle-cohorts-consumer")


ITER=0
for i in "${consumers[@]}"
do
   echo "$ITER.$i"
   ITER=$(expr $ITER + 1)
done

echo "CONSUMER_BUILD_NUMBER="$CONSUMER_BUILD_NUMBER

UPDATE_IMAGE=0
echo "Enter option"
read option
echo "Update image:"
echo "Yes: 1"
echo "NO: 0"
read UPDATE_IMAGE
echo "Enter scale dev"
read scaleDev
echo "Enter scale android prod"
read scaleProd
echo "Enter scale ios prod"
read scaleiOSProd


echo ${consumers[$option]}

case ${consumers[$option]} in
     ${consumers[0]})
        if [ $UPDATE_IMAGE -eq 1 ]; then
          kubectl set image deployment/persistence-consumer-product-dev persistence-consumer=<IMAGE_STORE_SERVER>/airlytics/consumer:$CONSUMER_BUILD_NUMBER --record -n $NAMESPACE
          kubectl set image deployment/persistence-consumer-product-prod persistence-consumer=<IMAGE_STORE_SERVER>/airlytics/consumer:$CONSUMER_BUILD_NUMBER --record -n $NAMESPACE
          kubectl set image deployment/persistence-consumer-product-dev persistence-consumer=<IMAGE_STORE_SERVER>/airlytics/consumer:$CONSUMER_BUILD_NUMBER --record -n $NAMESPACE
          kubectl set image deployment/persistence-consumer-product-prod persistence-consumer=<IMAGE_STORE_SERVER>/airlytics/consumer:$CONSUMER_BUILD_NUMBER --record -n $NAMESPACE

        fi
        kubectl scale --replicas=$scaleDev deployment/persistence-consumer-product-dev -n airlytics
        kubectl scale --replicas=$scaleProd deployment/persistence-consumer-product-prod -n airlytics
        kubectl scale --replicas=$scaleDev deployment/persistence-consumer-product-dev -n airlytics
        kubectl scale --replicas=$scaleiOSProd deployment/persistence-consumer-product-prod -n airlytics
        ;;
     *)
     ;;
esac

echo "Done"
