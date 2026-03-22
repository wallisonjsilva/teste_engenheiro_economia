"""
DAG 1 — Leitura dos XMLs NF-e e Envio ao Kafka.

Submete o script PySpark xml_to_kafka.py ao cluster Spark
para ler os 100 XMLs de NF-e e publicar como JSON no Kafka.

Ao final, aciona automaticamente a DAG 2 (dag_kafka_to_hive).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "engenheiro_dados",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_xml_to_kafka",
    default_args=default_args,
    description="DAG 1: Lê XMLs de NF-e e publica JSON no Kafka",
    schedule_interval="*/10 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nfe", "xml", "kafka", "ingestao"],
) as dag:

    xml_to_kafka = SparkSubmitOperator(
        task_id="spark_xml_to_kafka",
        application="/opt/spark/scripts/xml_to_kafka.py",
        conn_id="spark_default",
        verbose=True,
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.jars": (
                "/opt/spark/user-jars/spark-xml_2.12-0.18.0.jar,"
                "/opt/spark/user-jars/spark-sql-kafka-0-10_2.12-3.5.2.jar,"
                "/opt/spark/user-jars/kafka-clients-3.5.2.jar,"
                "/opt/spark/user-jars/spark-token-provider-kafka-0-10_2.12-3.5.2.jar,"
                "/opt/spark/user-jars/commons-pool2-2.12.0.jar"
            ),
        },
    )

    trigger_dag2 = TriggerDagRunOperator(
        task_id="trigger_dag_kafka_to_hive",
        trigger_dag_id="dag_kafka_to_hive",
        wait_for_completion=False,
    )

    xml_to_kafka >> trigger_dag2
