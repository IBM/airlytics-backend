#!/bin/bash
./gradlew bootJar

$(aws ecr get-login --no-include-email)

docker build -t 772723803090.dkr.ecr.eu-west-1.amazonaws.com/airlytics/userdb-periodical-process:$BUILD_NUMBER .
docker push 772723803090.dkr.ecr.eu-west-1.amazonaws.com/airlytics/userdb-periodical-process:$BUILD_NUMBER


aws eks --region eu-west-1  update-kubeconfig --name airlytics-dev
kubectl set image deployment/retention-tracker-db-handler retention-tracker-db-handler=772723803090.dkr.ecr.eu-west-1.amazonaws.com/airlytics/userdb-periodical-process:$BUILD_NUMBER --record -n airlytics
kubectl set image deployment/db-pruner db-pruner=772723803090.dkr.ecr.eu-west-1.amazonaws.com/airlytics/userdb-periodical-process:$BUILD_NUMBER --record -n airlytics
kubectl set image deployment/userdb-parquet-dumper userdb-parquet-dumper=772723803090.dkr.ecr.eu-west-1.amazonaws.com/airlytics/userdb-periodical-process:$BUILD_NUMBER --record -n airlytics
