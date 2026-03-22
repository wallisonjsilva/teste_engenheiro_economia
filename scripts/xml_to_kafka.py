"""
DAG 1 — PySpark: Leitura dos XMLs NF-e e publicação no Kafka como JSON.

Lê todos os arquivos XML da pasta /opt/spark/xmls/, extrai os campos
relevantes da NF-e (identificação, emitente, destinatário, itens, totais)
e publica cada nota como mensagem JSON no tópico Kafka 'nfe-json'.
"""

import json
import glob
import xml.etree.ElementTree as ET
from pyspark.sql import SparkSession

# Namespace padrão da NF-e SEFAZ
NS = {"nfe": "http://www.portalfiscal.inf.br/nfe"}

KAFKA_BOOTSTRAP_SERVERS = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
KAFKA_TOPIC = "nfe-json"
XML_DIR = "/opt/spark/xmls"


def parse_text(element, xpath, ns=NS):
    """Extrai texto de um elemento XML via xpath, retorna string vazia se não encontrar."""
    node = element.find(xpath, ns)
    return node.text.strip() if node is not None and node.text else ""


def parse_item(det_element):
    """Parseia um elemento <det> (item da nota fiscal)."""
    prod = det_element.find("nfe:prod", NS)
    imposto = det_element.find("nfe:imposto", NS)

    item = {
        "nItem": det_element.get("nItem", ""),
        "cProd": parse_text(prod, "nfe:cProd"),
        "xProd": parse_text(prod, "nfe:xProd"),
        "NCM": parse_text(prod, "nfe:NCM"),
        "CFOP": parse_text(prod, "nfe:CFOP"),
        "uCom": parse_text(prod, "nfe:uCom"),
        "qCom": parse_text(prod, "nfe:qCom"),
        "vUnCom": parse_text(prod, "nfe:vUnCom"),
        "vProd": parse_text(prod, "nfe:vProd"),
        "vDesc": parse_text(prod, "nfe:vDesc"),
    }

    if imposto is not None:
        item["vTotTrib"] = parse_text(imposto, "nfe:vTotTrib")

    return item


def parse_nfe_xml(xml_path):
    """Parseia um arquivo XML de NF-e e retorna um dicionário com os dados."""
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Navegar até infNFe
    nfe = root.find("nfe:NFe", NS)
    if nfe is None:
        return None

    inf_nfe = nfe.find("nfe:infNFe", NS)
    if inf_nfe is None:
        return None

    # Identificação (ide)
    ide = inf_nfe.find("nfe:ide", NS)

    # Emitente (emit)
    emit = inf_nfe.find("nfe:emit", NS)
    ender_emit = emit.find("nfe:enderEmit", NS) if emit is not None else None

    # Destinatário (dest)
    dest = inf_nfe.find("nfe:dest", NS)
    ender_dest = dest.find("nfe:enderDest", NS) if dest is not None else None

    # Totais
    total = inf_nfe.find("nfe:total", NS)
    icms_tot = total.find("nfe:ICMSTot", NS) if total is not None else None

    # Protocolo
    prot_nfe = root.find("nfe:protNFe", NS)
    inf_prot = prot_nfe.find("nfe:infProt", NS) if prot_nfe is not None else None

    # Itens (det)
    itens = []
    for det in inf_nfe.findall("nfe:det", NS):
        itens.append(parse_item(det))

    nfe_data = {
        # Chave da NF-e
        "chNFe": inf_nfe.get("Id", "").replace("NFe", ""),
        "versao": inf_nfe.get("versao", ""),

        # Identificação
        "ide": {
            "cUF": parse_text(ide, "nfe:cUF"),
            "cNF": parse_text(ide, "nfe:cNF"),
            "natOp": parse_text(ide, "nfe:natOp"),
            "mod": parse_text(ide, "nfe:mod"),
            "serie": parse_text(ide, "nfe:serie"),
            "nNF": parse_text(ide, "nfe:nNF"),
            "dhEmi": parse_text(ide, "nfe:dhEmi"),
            "dhSaiEnt": parse_text(ide, "nfe:dhSaiEnt"),
            "tpNF": parse_text(ide, "nfe:tpNF"),
            "cMunFG": parse_text(ide, "nfe:cMunFG"),
        },

        # Emitente
        "emit": {
            "CNPJ": parse_text(emit, "nfe:CNPJ") if emit is not None else "",
            "xNome": parse_text(emit, "nfe:xNome") if emit is not None else "",
            "xFant": parse_text(emit, "nfe:xFant") if emit is not None else "",
            "IE": parse_text(emit, "nfe:IE") if emit is not None else "",
            "CRT": parse_text(emit, "nfe:CRT") if emit is not None else "",
            "endereco": {
                "xLgr": parse_text(ender_emit, "nfe:xLgr") if ender_emit is not None else "",
                "nro": parse_text(ender_emit, "nfe:nro") if ender_emit is not None else "",
                "xBairro": parse_text(ender_emit, "nfe:xBairro") if ender_emit is not None else "",
                "cMun": parse_text(ender_emit, "nfe:cMun") if ender_emit is not None else "",
                "xMun": parse_text(ender_emit, "nfe:xMun") if ender_emit is not None else "",
                "UF": parse_text(ender_emit, "nfe:UF") if ender_emit is not None else "",
                "CEP": parse_text(ender_emit, "nfe:CEP") if ender_emit is not None else "",
            },
        },

        # Destinatário
        "dest": {
            "CPF": parse_text(dest, "nfe:CPF") if dest is not None else "",
            "CNPJ": parse_text(dest, "nfe:CNPJ") if dest is not None else "",
            "xNome": parse_text(dest, "nfe:xNome") if dest is not None else "",
            "endereco": {
                "xLgr": parse_text(ender_dest, "nfe:xLgr") if ender_dest is not None else "",
                "nro": parse_text(ender_dest, "nfe:nro") if ender_dest is not None else "",
                "xBairro": parse_text(ender_dest, "nfe:xBairro") if ender_dest is not None else "",
                "cMun": parse_text(ender_dest, "nfe:cMun") if ender_dest is not None else "",
                "xMun": parse_text(ender_dest, "nfe:xMun") if ender_dest is not None else "",
                "UF": parse_text(ender_dest, "nfe:UF") if ender_dest is not None else "",
                "CEP": parse_text(ender_dest, "nfe:CEP") if ender_dest is not None else "",
            },
        },

        # Itens / Produtos
        "itens": itens,

        # Totais
        "total": {
            "vBC": parse_text(icms_tot, "nfe:vBC") if icms_tot is not None else "0.00",
            "vICMS": parse_text(icms_tot, "nfe:vICMS") if icms_tot is not None else "0.00",
            "vICMSDeson": parse_text(icms_tot, "nfe:vICMSDeson") if icms_tot is not None else "0.00",
            "vBCST": parse_text(icms_tot, "nfe:vBCST") if icms_tot is not None else "0.00",
            "vST": parse_text(icms_tot, "nfe:vST") if icms_tot is not None else "0.00",
            "vProd": parse_text(icms_tot, "nfe:vProd") if icms_tot is not None else "0.00",
            "vFrete": parse_text(icms_tot, "nfe:vFrete") if icms_tot is not None else "0.00",
            "vSeg": parse_text(icms_tot, "nfe:vSeg") if icms_tot is not None else "0.00",
            "vDesc": parse_text(icms_tot, "nfe:vDesc") if icms_tot is not None else "0.00",
            "vII": parse_text(icms_tot, "nfe:vII") if icms_tot is not None else "0.00",
            "vIPI": parse_text(icms_tot, "nfe:vIPI") if icms_tot is not None else "0.00",
            "vPIS": parse_text(icms_tot, "nfe:vPIS") if icms_tot is not None else "0.00",
            "vCOFINS": parse_text(icms_tot, "nfe:vCOFINS") if icms_tot is not None else "0.00",
            "vOutro": parse_text(icms_tot, "nfe:vOutro") if icms_tot is not None else "0.00",
            "vNF": parse_text(icms_tot, "nfe:vNF") if icms_tot is not None else "0.00",
            "vTotTrib": parse_text(icms_tot, "nfe:vTotTrib") if icms_tot is not None else "0.00",
        },

        # Protocolo de autorização
        "protocolo": {
            "nProt": parse_text(inf_prot, "nfe:nProt") if inf_prot is not None else "",
            "dhRecbto": parse_text(inf_prot, "nfe:dhRecbto") if inf_prot is not None else "",
            "cStat": parse_text(inf_prot, "nfe:cStat") if inf_prot is not None else "",
        },
    }

    return nfe_data


def main():
    spark = SparkSession.builder \
        .appName("DAG1_XML_to_Kafka") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Listar todos os XMLs
    xml_files = sorted(glob.glob(f"{XML_DIR}/nfe_*.xml"))
    print(f"[DAG1] Encontrados {len(xml_files)} arquivos XML para processar.")

    if not xml_files:
        print("[DAG1] ERRO: Nenhum arquivo XML encontrado!")
        spark.stop()
        return

    # Parsear todos os XMLs
    nfe_records = []
    for xml_path in xml_files:
        try:
            nfe_data = parse_nfe_xml(xml_path)
            if nfe_data:
                nfe_records.append(nfe_data)
                print(f"[DAG1] Parseado: {xml_path} -> chNFe={nfe_data['chNFe'][:20]}...")
            else:
                print(f"[DAG1] AVISO: Não foi possível parsear {xml_path}")
        except Exception as e:
            print(f"[DAG1] ERRO ao parsear {xml_path}: {e}")

    print(f"[DAG1] Total de notas parseadas: {len(nfe_records)}")

    # Converter para DataFrame do Spark e publicar no Kafka
    # Cada registro vira uma linha com key=chNFe e value=JSON completo
    kafka_records = []
    for nfe in nfe_records:
        kafka_records.append((nfe["chNFe"], json.dumps(nfe, ensure_ascii=False)))

    df = spark.createDataFrame(kafka_records, ["key", "value"])

    print(f"[DAG1] Publicando {df.count()} mensagens no tópico Kafka '{KAFKA_TOPIC}'...")

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", KAFKA_TOPIC) \
        .save()

    print(f"[DAG1] Publicação concluída com sucesso!")
    spark.stop()


if __name__ == "__main__":
    main()
