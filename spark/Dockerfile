FROM apache/spark:4.0.2

USER root

# Instalando pacotes e os jars necessarios
# Usando apt-get pois a imagem apache/spark é baseada em debian/ubuntu
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

# Baixando os Jars para conversão XML e persistência via Iceberg, e conectores do Kafka
RUN wget -q https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/0.18.0/spark-xml_2.12-0.18.0.jar -P /opt/spark/jars/ \
    && wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar -P /opt/spark/jars/ \
    && wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar -P /opt/spark/jars/ \
    && wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar -P /opt/spark/jars/ \
    && wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar -P /opt/spark/jars/ \
    && wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar -P /opt/spark/jars/

USER spark
