FROM flink:1.20.0-scala_2.12-java11

# Install Python 3, pip, and a full JDK (PyFlink's pemja needs JDK headers to build)
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev openjdk-11-jdk && \
    rm -rf /var/lib/apt/lists/*

# Make `python` point to python3 (PyFlink expects `python`)
RUN ln -s /usr/bin/python3 /usr/bin/python

# Point the build at the JDK we just installed so pemja can find the headers
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64

# Upgrade pip, then install PyFlink matching the cluster version
RUN pip3 install --upgrade pip && \
    pip3 install apache-flink==1.20.0

# Download the Kafka SQL connector JAR into Flink's lib folder
RUN wget -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar
