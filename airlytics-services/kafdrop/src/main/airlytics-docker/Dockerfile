FROM amazoncorretto:11

RUN yum update kernel --assumeyes
RUN yum update libarchive --assumeyes
RUN yum update sqlite --assumeyes

ADD kafka.truststore.jks /
ADD kafdrop.sh /
ADD kafdrop*tar.gz /

RUN chmod +x /kafdrop.sh

ENTRYPOINT ["/kafdrop.sh"]
