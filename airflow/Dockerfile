FROM apache/airflow:2.8.1-python3.10

USER root
# Install Java to allow running SparkSubmit natively inside airflow container if necessary (local mode),
# although we'll probably submit to the remote spark cluster.
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

# Instalando dependencias extras necessarias
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-apache-hive \
    apache-airflow-providers-apache-kafka \
    pyspark==3.5.1
