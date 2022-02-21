#!/bin/bash

#load cluser config
source ./cluster.properties

# login into cluster
chmod +x login_cluster.sh
./login_cluster.sh
source ./cloud-watch-logs-dashboard.sh

## now loop through the service pods
dashboard_widgets=""
for i in "${services[@]}"
do
echo "add $i logs to dashboard"
dashboard_widgets=$dashboard_widgets'{"type": "log", "x": 0, "y": 0, "width": 24, "height": 6, "properties": {"query": "source '"'"'/aws/containerinsights/'$CLUSTER_NAME'/application'"'"' | fields @timestamp, log\n| sort @timestamp desc\n| limit 100\n| filter kubernetes.pod_name like \"'$i'\" and (@message like \"'ERROR'\" or @message like \"'error'\" or @message like \"'Error'\")", "region": "'$REGION'", "stacked": false, "title": "Log group: ('$i')", "view": "table"}},'
done

dashboard_widgets=$(sed 's/.\{1\}$//' <<< "$dashboard_widgets")
echo "$dashboard_widgets"

dashboard='{"widgets": ['$dashboard_widgets']}'
aws cloudwatch put-dashboard --dashboard-name services --dashboard-body "$dashboard" --profile $PROFILE



## now loop through the consumers pods
dashboard_widgets=""
for i in "${consumers[@]}"
do
echo "add $i logs to dashboard"
dashboard_widgets=$dashboard_widgets'{"type": "log", "x": 0, "y": 0, "width": 24, "height": 6, "properties": {"query": "source '"'"'/aws/containerinsights/'$CLUSTER_NAME'/application'"'"' | fields @timestamp, log\n| sort @timestamp desc\n| limit 100\n| filter kubernetes.pod_name like \"'$i'\" and (@message like \"'ERROR'\" or @message like \"'error'\" or @message like \"'Error'\")", "region": "'$REGION'", "stacked": false, "title": "Log group: ('$i')", "view": "table"}},'
done

dashboard_widgets=$(sed 's/.\{1\}$//' <<< "$dashboard_widgets")
echo "$dashboard_widgets"

dashboard='{"widgets": ['$dashboard_widgets']}'
aws cloudwatch put-dashboard --dashboard-name consumers --dashboard-body "$dashboard" --profile $PROFILE



