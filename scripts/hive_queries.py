"""
DAG 3 — PySpark/Spark SQL: Consultas analíticas sobre os dados no Hive.

Executa consultas SQL sobre as tabelas nfe_notas e nfe_itens,
salvando os resultados como novas tabelas Hive para análise:
  1. Total de notas por UF do emitente
  2. Faturamento total por emitente
  3. Top 20 produtos mais vendidos (por quantidade)
  4. Valor total de tributos por nota
  5. Valor médio das notas por mês de emissão
"""

from pyspark.sql import SparkSession

HIVE_METASTORE_URI = "thrift://hive-metastore:9083"
HIVE_DATABASE = "nfe_db"


def main():
    spark = SparkSession.builder \
        .appName("DAG3_Hive_Queries") \
        .master("spark://spark-master:7077") \
        .config("hive.metastore.uris", HIVE_METASTORE_URI) \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.sql(f"USE {HIVE_DATABASE}")

    print("[DAG3] Iniciando consultas analíticas...\n")

    # ============================================================
    # 1. Total de notas por UF do emitente
    # ============================================================
    print("=" * 60)
    print("[DAG3] Consulta 1: Total de notas por UF do emitente")
    print("=" * 60)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {HIVE_DATABASE}.resultado_notas_por_uf AS
        SELECT
            emit_UF                       AS uf_emitente,
            COUNT(*)                      AS total_notas,
            SUM(total_vNF)                AS valor_total,
            ROUND(AVG(total_vNF), 2)      AS valor_medio
        FROM {HIVE_DATABASE}.nfe_notas
        GROUP BY emit_UF
        ORDER BY total_notas DESC
    """)

    # Para reexecução: dropar e recriar
    spark.sql(f"DROP TABLE IF EXISTS {HIVE_DATABASE}.resultado_notas_por_uf")
    result_1 = spark.sql(f"""
        SELECT
            emit_UF                       AS uf_emitente,
            COUNT(*)                      AS total_notas,
            ROUND(SUM(total_vNF), 2)      AS valor_total,
            ROUND(AVG(total_vNF), 2)      AS valor_medio
        FROM {HIVE_DATABASE}.nfe_notas
        GROUP BY emit_UF
        ORDER BY total_notas DESC
    """)
    result_1.write.mode("overwrite").saveAsTable(f"{HIVE_DATABASE}.resultado_notas_por_uf")
    result_1.show(truncate=False)

    # ============================================================
    # 2. Faturamento total por emitente
    # ============================================================
    print("=" * 60)
    print("[DAG3] Consulta 2: Faturamento total por emitente")
    print("=" * 60)

    spark.sql(f"DROP TABLE IF EXISTS {HIVE_DATABASE}.resultado_faturamento_emitente")
    result_2 = spark.sql(f"""
        SELECT
            emit_CNPJ                     AS cnpj_emitente,
            emit_xNome                    AS nome_emitente,
            emit_xFant                    AS fantasia_emitente,
            emit_UF                       AS uf_emitente,
            COUNT(*)                      AS total_notas,
            ROUND(SUM(total_vProd), 2)    AS valor_produtos,
            ROUND(SUM(total_vNF), 2)      AS valor_total_nf,
            ROUND(SUM(total_vTotTrib), 2) AS valor_total_tributos
        FROM {HIVE_DATABASE}.nfe_notas
        GROUP BY emit_CNPJ, emit_xNome, emit_xFant, emit_UF
        ORDER BY valor_total_nf DESC
    """)
    result_2.write.mode("overwrite").saveAsTable(f"{HIVE_DATABASE}.resultado_faturamento_emitente")
    result_2.show(truncate=False)

    # ============================================================
    # 3. Produtos mais vendidos (por quantidade)
    # ============================================================
    print("=" * 60)
    print("[DAG3] Consulta 3: Top 20 produtos mais vendidos")
    print("=" * 60)

    spark.sql(f"DROP TABLE IF EXISTS {HIVE_DATABASE}.resultado_produtos_mais_vendidos")
    result_3 = spark.sql(f"""
        SELECT
            cProd                                AS codigo_produto,
            xProd                                AS descricao_produto,
            uCom                                 AS unidade,
            COUNT(*)                             AS qtd_vendas,
            ROUND(SUM(qCom), 4)                  AS quantidade_total,
            ROUND(SUM(vProd), 2)                 AS valor_total,
            ROUND(AVG(vUnCom), 2)                AS preco_medio_unitario
        FROM {HIVE_DATABASE}.nfe_itens
        GROUP BY cProd, xProd, uCom
        ORDER BY quantidade_total DESC
        LIMIT 20
    """)
    result_3.write.mode("overwrite").saveAsTable(f"{HIVE_DATABASE}.resultado_produtos_mais_vendidos")
    result_3.show(truncate=False)

    # ============================================================
    # 4. Valor total de tributos por nota
    # ============================================================
    print("=" * 60)
    print("[DAG3] Consulta 4: Valor total de tributos por nota")
    print("=" * 60)

    spark.sql(f"DROP TABLE IF EXISTS {HIVE_DATABASE}.resultado_tributos_por_nota")
    result_4 = spark.sql(f"""
        SELECT
            chNFe                                 AS chave_nfe,
            ide_nNF                               AS numero_nf,
            emit_xNome                            AS emitente,
            total_vNF                             AS valor_nota,
            total_vICMS                           AS valor_icms,
            total_vPIS                            AS valor_pis,
            total_vCOFINS                         AS valor_cofins,
            total_vTotTrib                        AS valor_total_tributos,
            CASE
                WHEN total_vNF > 0
                THEN ROUND((total_vTotTrib / total_vNF) * 100, 2)
                ELSE 0
            END                                   AS percentual_tributos
        FROM {HIVE_DATABASE}.nfe_notas
        ORDER BY valor_total_tributos DESC
    """)
    result_4.write.mode("overwrite").saveAsTable(f"{HIVE_DATABASE}.resultado_tributos_por_nota")
    result_4.show(truncate=False)

    # ============================================================
    # 5. Valor médio das notas por mês de emissão
    # ============================================================
    print("=" * 60)
    print("[DAG3] Consulta 5: Valor médio das notas por mês")
    print("=" * 60)

    spark.sql(f"DROP TABLE IF EXISTS {HIVE_DATABASE}.resultado_valor_medio_mensal")
    result_5 = spark.sql(f"""
        SELECT
            SUBSTRING(ide_dhEmi, 1, 7)            AS ano_mes,
            COUNT(*)                              AS total_notas,
            ROUND(SUM(total_vNF), 2)              AS valor_total,
            ROUND(AVG(total_vNF), 2)              AS valor_medio,
            ROUND(MIN(total_vNF), 2)              AS valor_minimo,
            ROUND(MAX(total_vNF), 2)              AS valor_maximo
        FROM {HIVE_DATABASE}.nfe_notas
        GROUP BY SUBSTRING(ide_dhEmi, 1, 7)
        ORDER BY ano_mes
    """)
    result_5.write.mode("overwrite").saveAsTable(f"{HIVE_DATABASE}.resultado_valor_medio_mensal")
    result_5.show(truncate=False)

    # ============================================================
    # Resumo final
    # ============================================================
    print("\n" + "=" * 60)
    print("[DAG3] RESUMO — Tabelas de resultados criadas:")
    print("=" * 60)

    for table in [
        "resultado_notas_por_uf",
        "resultado_faturamento_emitente",
        "resultado_produtos_mais_vendidos",
        "resultado_tributos_por_nota",
        "resultado_valor_medio_mensal",
    ]:
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {HIVE_DATABASE}.{table}").collect()[0]["cnt"]
        print(f"  - {HIVE_DATABASE}.{table}: {count} registros")

    print("\n[DAG3] Consultas analíticas concluídas com sucesso!")
    spark.stop()


if __name__ == "__main__":
    main()
