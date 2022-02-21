FROM amazoncorretto:8
WORKDIR /usr/src/app
RUN yum -y update nss
RUN yum -y update sqlite
RUN yum -y update nss-softokn
ADD https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem /root/.postgresql/root.crt
COPY ./build/libs/event-aggregator-job-1.0.jar ./
EXPOSE 8081
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=90.0", "-jar", "event-aggregator-job-1.0.jar"]
