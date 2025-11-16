FROM eclipse-temurin:8-jdk

ENV FLUME_VERSION=1.9.0
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=$PATH:$JAVA_HOME/bin

# Installer wget et curl
RUN apt-get update && apt-get install -y wget curl && rm -rf /var/lib/apt/lists/*

# Télécharger et installer Flume
RUN wget https://archive.apache.org/dist/flume/1.9.0/apache-flume-$FLUME_VERSION-bin.tar.gz && \
    tar -xzf apache-flume-$FLUME_VERSION-bin.tar.gz -C /opt && \
    mv /opt/apache-flume-$FLUME_VERSION-bin /opt/flume && \
    rm apache-flume-$FLUME_VERSION-bin.tar.gz

RUN wget https://downloads.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz && \
    tar -xzf hadoop-$HADOOP_VERSION.tar.gz -C /opt && \
    mv /opt/hadoop-$HADOOP_VERSION $HADOOP_HOME && \
    rm hadoop-$HADOOP_VERSION.tar.gz

# Ajouter Hadoop au classpath de Flume
ENV CLASSPATH=$FLUME_HOME/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*


# Ajouter Flume au PATH
ENV FLUME_HOME=/opt/flume
ENV PATH=$PATH:$FLUME_HOME/bin
WORKDIR $FLUME_HOME


