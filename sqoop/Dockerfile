FROM ubuntu:18.04
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk wget rsync ssh default-jre && \
    apt-get clean

RUN wget -q http://archive.apache.org/dist/sqoop/1.4.7/sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz -O /tmp/sqoop.tgz && \
    tar -xzf /tmp/sqoop.tgz -C /opt && \
    ln -s /opt/sqoop-1.4.7.bin__hadoop-2.6.0 /opt/sqoop

ENV SQOOP_HOME=/opt/sqoop
ENV PATH=$PATH:$SQOOP_HOME/bin
WORKDIR /root
