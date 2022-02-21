./gradlew bootJar
docker build -t airlytics/userdb-periodical-process .
docker run -e AIRLYTICS_ENVIRONMENT=DEV -p 8084:8084 -p 8082:8082 airlytics/userdb-periodical-process