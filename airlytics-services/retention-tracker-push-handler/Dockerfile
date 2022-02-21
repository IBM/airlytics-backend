FROM amazoncorretto:8
WORKDIR /usr/src/app
RUN yum -y update nss
RUN yum -y update sqlite
RUN yum -y update nss-softokn
ADD https://_location_/_of_/_rds-ca-root.pem_ /root/.postgresql/root.crt
COPY ./build/libs/retention-tracker-push-handler.jar ./
EXPOSE 8084
ENTRYPOINT ["java", "-XX:+UnlockExperimentalVMOptions", "-XX:+UseCGroupMemoryLimitForHeap", "-jar", "retention-tracker-push-handler.jar"]

