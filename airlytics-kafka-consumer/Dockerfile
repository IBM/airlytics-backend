FROM amazoncorretto:8
RUN yum -y update nss
RUN yum -y update sqlite
RUN yum -y update nss-softokn
RUN yum -y update openssl
WORKDIR /root
ADD https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem ./.postgresql/root.crt
WORKDIR /usr/src/app
COPY ./build/libs/AirlyticsConsumer-1.0.jar ./
COPY consumer_startup.sh ./
RUN chmod +x consumer_startup.sh
EXPOSE 8080
ENTRYPOINT ["./consumer_startup.sh"]

