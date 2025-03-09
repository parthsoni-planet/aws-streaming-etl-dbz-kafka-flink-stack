FROM flink:1.18.1-java17

# Install necessary packages to ensure the JNI header file is available
RUN apt-get update && apt-get install -y openjdk-17-jdk-headless

#Get JAR Dependencies
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.1.0-1.18/flink-connector-kafka-3.1.0-1.18.jar; \
  wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.2.3/kafka-clients-3.2.3.jar; \
  wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar; \
  wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar; 

#Install Python
RUN apt-get update -y && \
apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev libffi-dev liblzma-dev && \
wget https://www.python.org/ftp/python/3.10.13/Python-3.10.13.tgz && \
tar -xvf Python-3.10.13.tgz && \
cd Python-3.10.13 && \
./configure --without-tests --enable-shared && \
make -j6 && \
make install && \
ldconfig /usr/local/lib && \
cd .. && rm -f Python-3.10.13.tgz && rm -rf Python-3.10.13 && \
ln -s /usr/local/bin/python3 /usr/local/bin/python && \
apt-get clean && \
rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

#Install PyFlink
RUN pip3 install apache-flink-libraries==1.18.1 && \
pip3 install apache-flink==1.18.1 && \
pip3 install backports.lzma
