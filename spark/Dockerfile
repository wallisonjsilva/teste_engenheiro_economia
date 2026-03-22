# Base limpa com Python 3.10 (combina com seu Airflow)
FROM python:3.10-slim-bullseye

USER root

# 1. Instalar Java (Necessário para o Spark) e ferramentas de rede
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jre-headless \
    wget \
    curl \
    procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# 2. Instalar Spark 3.5.2 manualmente (Garante controle total)
ENV SPARK_VERSION=3.5.2
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# 3. Baixar os Jars necessários (Conversão XML, Kafka, Iceberg)
# Colocamos direto na pasta de Jars do Spark que instalamos
WORKDIR /opt/spark/jars
RUN wget -q https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/0.18.0/spark-xml_2.12-0.18.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.2/spark-sql-kafka-0-10_2.12-3.5.2.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.2/kafka-clients-3.5.2.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.2/spark-token-provider-kafka-0-10_2.12-3.5.2.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar

# 4. Instalar lxml para o Python 3.10
RUN pip install --no-cache-dir lxml pyspark==${SPARK_VERSION}

# Variáveis para o PySpark não se perder
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

WORKDIR /opt/spark