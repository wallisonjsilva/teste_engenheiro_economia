"""
DAG 2 — Consumo do Kafka e Persistência no Hive.

Submete o script PySpark kafka_to_hive.py ao cluster Spark
para consumir mensagens JSON do Kafka e persistir no Hive.

É acionada automaticamente pela DAG 1 via TriggerDagRunOperator.
Ao final, aciona a DAG 3 (dag_hive_queries).
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
    dag_id="dag_kafka_to_hive",
    default_args=default_args,
    description="DAG 2: Consome JSON do Kafka e persiste no Hive",
    schedule_interval=None,  # Acionada pela DAG 1
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nfe", "kafka", "hive", "processamento"],
) as dag:

    kafka_to_hive = SparkSubmitOperator(
        task_id="spark_kafka_to_hive",
        application="/opt/spark/scripts/kafka_to_hive.py",
        conn_id="spark_default",
        verbose=True,
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.jars": (
                "/opt/spark/user-jars/spark-sql-kafka-0-10_2.12-3.5.2.jar,"
                "/opt/spark/user-jars/kafka-clients-3.5.2.jar,"
                "/opt/spark/user-jars/spark-token-provider-kafka-0-10_2.12-3.5.2.jar,"
                "/opt/spark/user-jars/commons-pool2-2.12.0.jar"
            ),
            "spark.sql.catalogImplementation": "hive",
            "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore:9083",
            "spark.sql.warehouse.dir": "hdfs://namenode:9000/user/hive/warehouse",
        },
    )

    trigger_dag3 = TriggerDagRunOperator(
        task_id="trigger_dag_hive_queries",
        trigger_dag_id="dag_hive_queries",
        wait_for_completion=False,
    )

    kafka_to_hive >> trigger_dag3
