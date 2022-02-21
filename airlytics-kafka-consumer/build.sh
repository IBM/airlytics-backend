#!/bin/bash
./gradlew fatJar

$(aws ecr get-login --no-include-email)
docker build --no-cache -t 772723803090.dkr.ecr.eu-west-1.amazonaws.com/airlytics/consumer:$BUILD_NUMBER .
docker push 772723803090.dkr.ecr.eu-west-1.amazonaws.com/airlytics/consumer:$BUILD_NUMBER





