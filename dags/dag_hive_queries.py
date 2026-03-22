"""
DAG 3 — Consultas Analíticas no Hive.

Submete o script PySpark hive_queries.py ao cluster Spark
para executar consultas analíticas e salvar resultados no Hive.

É acionada automaticamente pela DAG 2 via TriggerDagRunOperator.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "engenheiro_dados",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_hive_queries",
    default_args=default_args,
    description="DAG 3: Executa consultas analíticas e salva resultados no Hive",
    schedule_interval=None,  # Acionada pela DAG 2
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nfe", "hive", "analytics", "consultas"],
) as dag:

    hive_queries = SparkSubmitOperator(
        task_id="spark_hive_queries",
        application="/opt/spark/scripts/hive_queries.py",
        conn_id="spark_default",
        verbose=True,
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.sql.catalogImplementation": "hive",
            "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore:9083",
            "spark.sql.warehouse.dir": "hdfs://namenode:9000/user/hive/warehouse",
        },
    )

    hive_queries
