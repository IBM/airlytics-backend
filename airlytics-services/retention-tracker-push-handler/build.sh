#!/bin/bash
./gradlew bootJar

$(aws ecr get-login --no-include-email)

docker build -t 772723803090.dkr.ecr.eu-west-1.amazonaws.com/airlytics/retention-tracker-push-handler:$BUILD_NUMBER .
docker push 772723803090.dkr.ecr.eu-west-1.amazonaws.com/airlytics/retention-tracker-push-handler:$BUILD_NUMBER


aws eks --region eu-west-1  update-kubeconfig --name airlytics-dev
kubectl set image deployment/retention-tracker-push-handler retention-tracker-push-handler=772723803090.dkr.ecr.eu-west-1.amazonaws.com/airlytics/retention-tracker-push-handler:$BUILD_NUMBER --record -n airlytics
