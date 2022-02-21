./gradlew bootJar
docker build -t airlytics/retention-tracker-push-handler .
docker run -e AIRLYTICS_ENVIRONMENT=DEV -p 8084:8084 airlytics/retention-tracker-push-handler