# airlytics-kubernetes-deployment
Kubernetes deployment configuration for Airlytics project.


### Update production system
The following section includes instructions a develop has to 
perform to complete the proper update of production system.

### Upgrade Airlytics component image version 
All project components are broken into two main categories:
 
    1. Airlytics Kafka-Consumer/Service
    2. Infrastructure components 
	
	
#### Infrastructure components

    
**Folder** | **Desciption** | **Link**
 --- | --- | --- |
/amazon-cloudwatch | FluentD to collect logs from EKS containers | https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/Container-Insights-setup-logs.html
/common/nodelocaldns.yaml | Nodelocal DNS Cache | https://github.com/kubernetes/kubernetes/tree/master/cluster/addons/dns/nodelocaldns
/kafka-scripts | helper scripts for Kafka topics management | |
/kafdrop | Kafdrop is a web UI for viewing Kafka topics and browsing consumer groups | https://github.com/obsidiandynamics/kafdrop
/kubernetes-dashboard | Kubernetes Dashboard is a general purpose, web-based UI for Kubernetes clusters | https://github.com/kubernetes/dashboard
/alert/slack/ | CloudWatch log forwarder to Slack channel as AWS lambda function | https://medium.com/pixelpoint/how-to-send-aws-cloudwatch-alarms-to-slack-502bcf106047
/efs-provider | Amazon EFS CSI driver | https://docs.aws.amazon.com/eks/latest/userguide/efs-csi.html 
/prometheus |  Prometheus and related monitoring components | https://github.com/prometheus-operator/prometheus-operator
/scripts | Operational scripts




 Airlytics Component:
 
 **Name** | **Desciption** | **link**
 --- | --- | --- |
 event-proxy| Event Proxy - Airlytics event format validator | [airlytics-event-proxy](https://github.com/TheWeatherCompany/airlytics-event-proxy)
 consumers | The process to consume events from the Kafka topic  and persist them in parquet file on S3. Separate instance for each platform | [persistence-consumer](https://github.com/TheWeatherCompany/airlytics-kafka-consumer)






#### Deploy kubernetes cluster on AWS EKS service

1 . Login to the already configured cluster 
 
```shell script
 aws eks --region eu-west-1  update-kubeconfig --name }  <CLUSTER_NAME>
```

2 . Create name space 

```shell script
 kubectl apply -f manifest/namespace.yaml 
```

3 . Create Prometheus 

```shell script
 kubectl apply -f prometheus/manifest/setup
```

```shell script
 kubectl apply -f prometheus/manifest
```

4 . Create event-proxy

```shell script
 kubectl apply -f event-proxy
```




### Start Kubernetes Dashboard UI

Retrieve an authentication token for the eks-admin service account. Copy the <authentication_token> value from the output. You use this token to connect to the dashboard.

```shell script
kubectl -n kube-system describe secret $(kubectl -n kube-system get secret | grep eks-admin | awk '{print $1}')
```

Start the kubectl proxy.

```shell script
kubectl proxy
```


To access the dashboard endpoint, open the following link with a web browser:
http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/#!/login.


Choose Token, paste the <authentication_token> output from the previous command into the Token field, and choose SIGN IN.





    1. airlytics/consumer 
    2. airlytics/event-proxy	
    3. airlytics/retention-tracker-push-handler	
    4. airlytics/userdb-periodical-process	
    5. amazoncorretto-nodejs	
    6. kafdrop-airlytics 