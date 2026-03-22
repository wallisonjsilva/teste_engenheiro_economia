"""
DAG 2 — PySpark: Consumo das mensagens do Kafka e persistência no Hive.

Consome mensagens JSON do tópico 'nfe-json', parseia os dados das NF-e
e persiste em duas tabelas Hive:
  - nfe_notas: dados gerais da nota (emitente, destinatário, totais)
  - nfe_itens: itens/produtos de cada nota (com chave da NF-e)
"""

import json
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    ArrayType, IntegerType
)
from pyspark.sql.functions import (
    col, from_json, explode, lit
)

KAFKA_BOOTSTRAP_SERVERS = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
KAFKA_TOPIC = "nfe-json"
HIVE_METASTORE_URI = "thrift://hive-metastore:9083"
HIVE_DATABASE = "nfe_db"


def get_nfe_schema():
    """Schema do JSON da NF-e publicado pelo DAG 1."""
    endereco_schema = StructType([
        StructField("xLgr", StringType()),
        StructField("nro", StringType()),
        StructField("xBairro", StringType()),
        StructField("cMun", StringType()),
        StructField("xMun", StringType()),
        StructField("UF", StringType()),
        StructField("CEP", StringType()),
    ])

    ide_schema = StructType([
        StructField("cUF", StringType()),
        StructField("cNF", StringType()),
        StructField("natOp", StringType()),
        StructField("mod", StringType()),
        StructField("serie", StringType()),
        StructField("nNF", StringType()),
        StructField("dhEmi", StringType()),
        StructField("dhSaiEnt", StringType()),
        StructField("tpNF", StringType()),
        StructField("cMunFG", StringType()),
    ])

    emit_schema = StructType([
        StructField("CNPJ", StringType()),
        StructField("xNome", StringType()),
        StructField("xFant", StringType()),
        StructField("IE", StringType()),
        StructField("CRT", StringType()),
        StructField("endereco", endereco_schema),
    ])

    dest_schema = StructType([
        StructField("CPF", StringType()),
        StructField("CNPJ", StringType()),
        StructField("xNome", StringType()),
        StructField("endereco", endereco_schema),
    ])

    item_schema = StructType([
        StructField("nItem", StringType()),
        StructField("cProd", StringType()),
        StructField("xProd", StringType()),
        StructField("NCM", StringType()),
        StructField("CFOP", StringType()),
        StructField("uCom", StringType()),
        StructField("qCom", StringType()),
        StructField("vUnCom", StringType()),
        StructField("vProd", StringType()),
        StructField("vDesc", StringType()),
        StructField("vTotTrib", StringType()),
    ])

    total_schema = StructType([
        StructField("vBC", StringType()),
        StructField("vICMS", StringType()),
        StructField("vICMSDeson", StringType()),
        StructField("vBCST", StringType()),
        StructField("vST", StringType()),
        StructField("vProd", StringType()),
        StructField("vFrete", StringType()),
        StructField("vSeg", StringType()),
        StructField("vDesc", StringType()),
        StructField("vII", StringType()),
        StructField("vIPI", StringType()),
        StructField("vPIS", StringType()),
        StructField("vCOFINS", StringType()),
        StructField("vOutro", StringType()),
        StructField("vNF", StringType()),
        StructField("vTotTrib", StringType()),
    ])

    protocolo_schema = StructType([
        StructField("nProt", StringType()),
        StructField("dhRecbto", StringType()),
        StructField("cStat", StringType()),
    ])

    nfe_schema = StructType([
        StructField("chNFe", StringType()),
        StructField("versao", StringType()),
        StructField("ide", ide_schema),
        StructField("emit", emit_schema),
        StructField("dest", dest_schema),
        StructField("itens", ArrayType(item_schema)),
        StructField("total", total_schema),
        StructField("protocolo", protocolo_schema),
    ])

    return nfe_schema


def main():
    spark = SparkSession.builder \
        .appName("DAG2_Kafka_to_Hive") \
        .master("spark://spark-master:7077") \
        .config("hive.metastore.uris", HIVE_METASTORE_URI) \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"[DAG2] Consumindo mensagens do tópico Kafka '{KAFKA_TOPIC}'...")

    # Ler do Kafka em modo batch (subscribe + read)
    kafka_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    print(f"[DAG2] Mensagens lidas do Kafka: {kafka_df.count()}")

    # Extrair o valor (JSON string) e parsear
    nfe_schema = get_nfe_schema()

    nfe_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), nfe_schema).alias("nfe")) \
        .select("nfe.*")

    print(f"[DAG2] Notas parseadas: {nfe_df.count()}")

    # ============================================================
    # Tabela 1: nfe_notas — dados gerais da nota
    # ============================================================
    notas_df = nfe_df.select(
        col("chNFe"),
        col("versao"),
        # Identificação
        col("ide.cUF").alias("ide_cUF"),
        col("ide.nNF").alias("ide_nNF"),
        col("ide.natOp").alias("ide_natOp"),
        col("ide.serie").alias("ide_serie"),
        col("ide.dhEmi").alias("ide_dhEmi"),
        col("ide.dhSaiEnt").alias("ide_dhSaiEnt"),
        col("ide.tpNF").alias("ide_tpNF"),
        col("ide.cMunFG").alias("ide_cMunFG"),
        # Emitente
        col("emit.CNPJ").alias("emit_CNPJ"),
        col("emit.xNome").alias("emit_xNome"),
        col("emit.xFant").alias("emit_xFant"),
        col("emit.IE").alias("emit_IE"),
        col("emit.CRT").alias("emit_CRT"),
        col("emit.endereco.xMun").alias("emit_xMun"),
        col("emit.endereco.UF").alias("emit_UF"),
        # Destinatário
        col("dest.CPF").alias("dest_CPF"),
        col("dest.CNPJ").alias("dest_CNPJ"),
        col("dest.xNome").alias("dest_xNome"),
        col("dest.endereco.xMun").alias("dest_xMun"),
        col("dest.endereco.UF").alias("dest_UF"),
        # Totais
        col("total.vBC").cast(DoubleType()).alias("total_vBC"),
        col("total.vICMS").cast(DoubleType()).alias("total_vICMS"),
        col("total.vProd").cast(DoubleType()).alias("total_vProd"),
        col("total.vFrete").cast(DoubleType()).alias("total_vFrete"),
        col("total.vDesc").cast(DoubleType()).alias("total_vDesc"),
        col("total.vPIS").cast(DoubleType()).alias("total_vPIS"),
        col("total.vCOFINS").cast(DoubleType()).alias("total_vCOFINS"),
        col("total.vNF").cast(DoubleType()).alias("total_vNF"),
        col("total.vTotTrib").cast(DoubleType()).alias("total_vTotTrib"),
        # Protocolo
        col("protocolo.nProt").alias("prot_nProt"),
        col("protocolo.dhRecbto").alias("prot_dhRecbto"),
        col("protocolo.cStat").alias("prot_cStat"),
    )

    # Criar database se não existir
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DATABASE}")
    spark.sql(f"USE {HIVE_DATABASE}")

    print(f"[DAG2] Persistindo tabela '{HIVE_DATABASE}.nfe_notas'...")
    notas_df.write.mode("overwrite").saveAsTable(f"{HIVE_DATABASE}.nfe_notas")
    print(f"[DAG2] Tabela nfe_notas salva com {notas_df.count()} registros.")

    # ============================================================
    # Tabela 2: nfe_itens — itens da nota (exploded)
    # ============================================================
    itens_df = nfe_df.select(
        col("chNFe"),
        col("ide.nNF").alias("nNF"),
        explode(col("itens")).alias("item")
    ).select(
        col("chNFe"),
        col("nNF"),
        col("item.nItem").cast(IntegerType()).alias("nItem"),
        col("item.cProd").alias("cProd"),
        col("item.xProd").alias("xProd"),
        col("item.NCM").alias("NCM"),
        col("item.CFOP").alias("CFOP"),
        col("item.uCom").alias("uCom"),
        col("item.qCom").cast(DoubleType()).alias("qCom"),
        col("item.vUnCom").cast(DoubleType()).alias("vUnCom"),
        col("item.vProd").cast(DoubleType()).alias("vProd"),
        col("item.vDesc").cast(DoubleType()).alias("vDesc"),
        col("item.vTotTrib").cast(DoubleType()).alias("vTotTrib"),
    )

    print(f"[DAG2] Persistindo tabela '{HIVE_DATABASE}.nfe_itens'...")
    itens_df.write.mode("overwrite").saveAsTable(f"{HIVE_DATABASE}.nfe_itens")
    print(f"[DAG2] Tabela nfe_itens salva com {itens_df.count()} registros.")

    # Verificação rápida
    print("\n[DAG2] === Verificação ===")
    spark.sql(f"SELECT COUNT(*) as total_notas FROM {HIVE_DATABASE}.nfe_notas").show()
    spark.sql(f"SELECT COUNT(*) as total_itens FROM {HIVE_DATABASE}.nfe_itens").show()

    print("[DAG2] Persistência concluída com sucesso!")
    spark.stop()


if __name__ == "__main__":
    main()
