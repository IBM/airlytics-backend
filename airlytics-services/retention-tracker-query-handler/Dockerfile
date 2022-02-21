FROM amazoncorretto:8
WORKDIR /usr/src/app
RUN yum -y update nss
RUN yum -y update sqlite 
RUN yum -y update nss-softokn
ADD https://xx.xxx.xxx/xxxxxxxxxx/xxxx.pem /root/.postgresql/root.crt
COPY ./build/libs/retention-tracker-query-handler.jar ./
EXPOSE 8084
EXPOSE 8082
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=90.0", "-jar", "retention-tracker-query-handler.jar"]

