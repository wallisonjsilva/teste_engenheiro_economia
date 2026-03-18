#!/usr/bin/env python3
"""
Gerador de 100 arquivos XML de NF-e com dados fake.
Estrutura baseada no arquivo nf_real.xml (formato nfeProc versao 3.10).
Cenário: venda de produtos de supermercado a consumidor final (pessoa física).
"""

import os
import random
import base64
import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from xml.dom.minidom import parseString

from faker import Faker

fake = Faker("pt_BR")
random.seed()

OUTPUT_DIR = "xmls"
NAMESPACE = "http://www.portalfiscal.inf.br/nfe"

ET.register_namespace("", NAMESPACE)

UF_CODES = {
    "AC": 12, "AL": 27, "AP": 16, "AM": 13, "BA": 29, "CE": 23,
    "DF": 53, "ES": 32, "GO": 52, "MA": 21, "MT": 51, "MS": 50,
    "MG": 31, "PA": 15, "PB": 25, "PR": 41, "PE": 26, "PI": 22,
    "RJ": 33, "RN": 24, "RS": 43, "RO": 11, "RR": 14, "SC": 42,
    "SP": 35, "SE": 28, "TO": 17,
}

# CFOP 5102: venda de mercadoria adquirida de terceiros a consumidor final (dentro do estado)
CFOP_VENDA_CONSUMIDOR = "5102"

# natOp fixa para venda de mercadoria a consumidor final
NAT_OP = "VENDA DE MERCADORIA"

# CNAE de supermercado: comércio varejista de mercadorias em geral
# com predominância de produtos alimentícios
CNAE_SUPERMERCADO = "4711302"

COND_PGTO_OPTIONS = [
    "1-A VISTA DINHEIRO",
    "2-A VISTA CARTAO CREDITO",
    "3-A VISTA CARTAO DEBITO",
    "4-PARCELADO SEM JUROS",
    "5-PARCELADO COM JUROS",
]

SUPERMARKET_SUFFIXES = [
    "SUPERMERCADO", "MERCADO", "SUPERMERCADOS", "ATACAREJO",
    "HIPERMERCADO", "MERCADINHO", "MERCEARIA",
]

# Catálogo de produtos de supermercado:
# (descricao, NCM, unidade, preco_min, preco_max, qtd_min, qtd_max)
PRODUCT_CATALOG = [
    # Grãos e cereais
    ("ARROZ BRANCO TIPO 1 5KG",          "10063090", "UN",  20.00,  38.00, 1, 5),
    ("FEIJAO CARIOCA 1KG",               "07133310", "UN",   6.00,  14.00, 1, 6),
    ("FEIJAO PRETO 1KG",                 "07133310", "UN",   7.00,  15.00, 1, 4),
    ("LENTILHA 500G",                    "07134010", "UN",   5.00,  10.00, 1, 3),
    ("AVEIA EM FLOCOS 500G",             "11042200", "UN",   6.00,  12.00, 1, 3),
    # Farinhas e massas
    ("FARINHA DE TRIGO 1KG",             "11010010", "UN",   4.00,   8.00, 1, 5),
    ("MACARRAO ESPAGUETE 500G",          "19021900", "UN",   3.00,   7.00, 1, 6),
    ("MACARRAO PARAFUSO 500G",           "19021900", "UN",   3.00,   7.00, 1, 6),
    ("FARINHA DE MANDIOCA 1KG",          "11062000", "UN",   5.00,  10.00, 1, 4),
    ("AMIDO DE MILHO 500G",              "11081200", "UN",   4.00,   8.00, 1, 3),
    # Óleos e condimentos
    ("OLEO DE SOJA 900ML",               "15079019", "UN",   7.00,  14.00, 1, 6),
    ("AZEITE DE OLIVA EXTRA VIRGEM 500ML","15091000", "UN",  25.00,  60.00, 1, 3),
    ("SAL REFINADO IODADO 1KG",          "25010020", "UN",   2.00,   4.00, 1, 4),
    ("ACUCAR CRISTAL 1KG",               "17019900", "UN",   4.00,   9.00, 1, 6),
    ("ACUCAR REFINADO 1KG",              "17019900", "UN",   5.00,  10.00, 1, 5),
    ("VINAGRE DE ALCOOL 750ML",          "22090000", "UN",   3.00,   6.00, 1, 3),
    ("MOLHO DE TOMATE 340G",             "21039090", "UN",   3.00,   6.00, 1, 8),
    ("EXTRATO DE TOMATE 340G",           "20029000", "UN",   3.50,   6.50, 1, 6),
    # Café e achocolatados
    ("CAFE TORRADO MOIDO 500G",          "09011190", "UN",  16.00,  38.00, 1, 4),
    ("CAFE SOLUVEL 200G",                "21011100", "UN",  18.00,  40.00, 1, 3),
    ("ACHOCOLATADO EM PO 400G",          "18063190", "UN",  12.00,  25.00, 1, 3),
    # Laticínios
    ("LEITE INTEGRAL UHT 1L",            "04011020", "UN",   4.00,   8.00, 1, 12),
    ("LEITE DESNATADO UHT 1L",           "04011010", "UN",   4.50,   9.00, 1, 8),
    ("MANTEIGA COM SAL 200G",            "04051000", "UN",  10.00,  20.00, 1, 4),
    ("MARGARINA 500G",                   "04033900", "UN",   8.00,  16.00, 1, 4),
    ("IOGURTE NATURAL 170G",             "04031000", "UN",   3.00,   6.00, 1, 6),
    ("REQUEIJAO CREMOSO 200G",           "04063000", "UN",   8.00,  16.00, 1, 4),
    ("QUEIJO MUSSARELA KG",              "04061000", "KG",  35.00,  60.00, 0.2, 2.0),
    ("QUEIJO PRATO KG",                  "04061000", "KG",  38.00,  65.00, 0.2, 2.0),
    # Frios e embutidos
    ("PRESUNTO COZIDO 200G",             "16010000", "UN",   8.00,  16.00, 1, 4),
    ("MORTADELA 200G",                   "16010000", "UN",   6.00,  12.00, 1, 4),
    ("LINGUICA TOSCANA 500G",            "16010000", "UN",  12.00,  22.00, 1, 3),
    ("SALSICHA 500G",                    "16010000", "UN",   8.00,  15.00, 1, 4),
    # Carnes e aves
    ("PEITO DE FRANGO KG",               "02071200", "KG",  12.00,  22.00, 0.5, 3.0),
    ("COXA E SOBRECOXA KG",              "02071300", "KG",  10.00,  18.00, 0.5, 3.0),
    ("CARNE BOVINA ACEM KG",             "02013000", "KG",  28.00,  55.00, 0.3, 2.0),
    ("CARNE BOVINA ALCATRA KG",          "02013000", "KG",  45.00,  85.00, 0.3, 2.0),
    ("CARNE SUINA LOMBO KG",             "02031900", "KG",  22.00,  40.00, 0.3, 2.0),
    ("PEIXE TILAPIA KG",                 "03025400", "KG",  20.00,  38.00, 0.3, 2.0),
    # Hortifrúti
    ("BANANA PRATA KG",                  "08030090", "KG",   3.00,   8.00, 0.5, 3.0),
    ("MACA FUJI KG",                     "08081000", "KG",   6.00,  14.00, 0.5, 2.0),
    ("TOMATE KG",                        "07020010", "KG",   4.00,  10.00, 0.5, 2.0),
    ("CEBOLA KG",                        "07031010", "KG",   3.00,   8.00, 0.5, 3.0),
    ("BATATA INGLESA KG",                "07019000", "KG",   4.00,  10.00, 0.5, 3.0),
    # Padaria e biscoitos
    ("PAO FRANCES KG",                   "19052090", "KG",  12.00,  20.00, 0.2, 1.0),
    ("PAO DE FORMA INTEGRAL 500G",       "19051000", "UN",   8.00,  16.00, 1, 3),
    ("BISCOITO RECHEADO 130G",           "19053200", "UN",   3.00,   6.00, 1, 6),
    ("BISCOITO CREAM CRACKER 200G",      "19053100", "UN",   4.00,   8.00, 1, 4),
    ("BOLACHA MAIZENA 200G",             "19053100", "UN",   4.00,   8.00, 1, 4),
    # Bebidas
    ("AGUA MINERAL SEM GAS 1,5L",        "22011000", "UN",   2.00,   5.00, 1, 12),
    ("REFRIGERANTE COLA 2L",             "22021000", "UN",   8.00,  14.00, 1, 6),
    ("REFRIGERANTE GUARANA 2L",          "22021000", "UN",   7.00,  13.00, 1, 6),
    ("SUCO DE LARANJA 1L",               "20092100", "UN",   6.00,  12.00, 1, 6),
    ("SUCO INTEGRAL CAIXINHA 200ML",     "20092900", "UN",   2.00,   5.00, 1, 12),
    ("CERVEJA LATA 350ML",               "22030000", "UN",   3.50,   7.00, 1, 12),
    ("CERVEJA LONG NECK 355ML",          "22030000", "UN",   5.00,  10.00, 1, 6),
    ("VINHO TINTO SECO 750ML",           "22042100", "UN",  25.00,  80.00, 1, 3),
    # Limpeza
    ("DETERGENTE LIQUIDO 500ML",         "34022090", "UN",   2.00,   5.00, 1, 6),
    ("SABAO EM PO 1KG",                  "34012090", "UN",  12.00,  22.00, 1, 4),
    ("AMACIANTE DE ROUPAS 2L",           "38091000", "UN",  14.00,  28.00, 1, 3),
    ("AGUA SANITARIA 1L",                "28281000", "UN",   3.00,   6.00, 1, 6),
    ("DESINFETANTE 500ML",               "38089290", "UN",   4.00,   9.00, 1, 4),
    ("ESPONJA DE COZINHA PCT 3UN",       "39262000", "PCT",  4.00,   8.00, 1, 4),
    ("PAPEL TOALHA 2 ROLOS",             "48182000", "PCT",  8.00,  16.00, 1, 4),
    ("PAPEL HIGIENICO 12 ROLOS",         "48182000", "PCT", 15.00,  30.00, 1, 4),
    ("SACO DE LIXO 50L PCT 10UN",        "39232990", "PCT",  6.00,  12.00, 1, 4),
    # Higiene pessoal
    ("SHAMPOO 400ML",                    "33051000", "UN",  12.00,  28.00, 1, 3),
    ("CONDICIONADOR 400ML",              "33057000", "UN",  12.00,  28.00, 1, 3),
    ("SABONETE EM BARRA 90G",            "34011190", "UN",   3.00,   7.00, 1, 6),
    ("CREME DENTAL 90G",                 "33061000", "UN",   5.00,  12.00, 1, 4),
    ("FIO DENTAL 100M",                  "33619000", "UN",   4.00,   9.00, 1, 3),
    ("DESODORANTE ROLL-ON 50ML",         "33079000", "UN",  10.00,  22.00, 1, 3),
    ("ABSORVENTE PCT 8UN",               "96190000", "PCT",  6.00,  14.00, 1, 4),
    ("FRALDAS DESCARTAVEIS PCT 16UN",    "96190000", "PCT", 25.00,  55.00, 1, 3),
    # Mercearia e conservas
    ("ATUM EM OLEO 170G",                "16041400", "UN",   6.00,  12.00, 1, 6),
    ("SARDINHA EM OLEO 125G",            "16041300", "UN",   4.00,   8.00, 1, 6),
    ("MILHO VERDE LATA 200G",            "20059900", "UN",   3.00,   6.00, 1, 6),
    ("ERVILHA LATA 200G",                "20059900", "UN",   3.00,   6.00, 1, 6),
    ("PALMITO PUPUNHA 300G",             "20089900", "UN",  12.00,  22.00, 1, 3),
    ("MEL PURO 500G",                    "04090000", "UN",  18.00,  40.00, 1, 3),
    ("CHOCOLATE AO LEITE 100G",          "18069000", "UN",   5.00,  12.00, 1, 6),
    ("ACHOCOLATADO LIQUIDO 200ML",       "18063190", "UN",   2.00,   4.50, 1, 12),
]


def tag(name: str) -> str:
    return f"{{{NAMESPACE}}}{name}"


def only_digits(value: str) -> str:
    return "".join(filter(str.isdigit, str(value)))


def calc_check_digit(chave43: str) -> int:
    """Calcula o dígito verificador da chave de acesso da NF-e (módulo 11)."""
    weights = ([2, 3, 4, 5, 6, 7, 8, 9] * 6)[:43]
    total = sum(int(d) * w for d, w in zip(reversed(chave43), weights))
    remainder = total % 11
    return 0 if remainder < 2 else 11 - remainder


def generate_nfe_key(cuf: int, aamm: str, cnpj: str, mod: int,
                     serie: int, nnf: int, tpemis: int, cnf: int) -> str:
    chave43 = (
        f"{cuf:02d}{aamm}{cnpj}{mod:02d}{serie:03d}{nnf:09d}{tpemis}{cnf:08d}"
    )
    cdv = calc_check_digit(chave43)
    return chave43 + str(cdv)


def sub(parent: ET.Element, name: str, text: str = None,
        attrib: dict = None) -> ET.Element:
    el = ET.SubElement(parent, tag(name), attrib or {})
    if text is not None:
        el.text = str(text)
    return el


def build_items(num_items: int) -> tuple:
    """Gera lista de itens de supermercado e retorna junto com os totais."""
    items = []
    total_prod = 0.0
    total_desc = 0.0
    total_trib = 0.0

    # Evita repetir o mesmo produto na mesma nota
    catalog_sample = random.sample(PRODUCT_CATALOG, min(num_items, len(PRODUCT_CATALOG)))

    for xprod, ncm, unit, pmin, pmax, qmin, qmax in catalog_sample:
        unit_price = round(random.uniform(pmin, pmax), 4)
        qty        = round(random.uniform(qmin, qmax), 4)

        # Arredonda para 3 casas em KG, inteiro em UN/PCT
        if unit in ("UN", "PCT", "CX", "DZ"):
            qty = float(int(max(1, round(qty))))

        v_prod   = round(qty * unit_price, 2)
        # Desconto ocorre em ~30% das vezes (promoção)
        v_desc   = round(v_prod * random.uniform(0.01, 0.08), 2) if random.random() < 0.30 else 0.0
        v_trib   = round(v_prod * 0.0585, 2)

        total_prod += v_prod
        total_desc += v_desc
        total_trib += v_trib

        items.append({
            "cProd":      str(random.randint(100, 9999)),
            "xProd":      xprod,
            "NCM":        ncm,
            "CFOP":       CFOP_VENDA_CONSUMIDOR,
            "unit":       unit,
            "qty":        qty,
            "unit_price": unit_price,
            "v_prod":     v_prod,
            "v_desc":     v_desc,
            "v_trib":     v_trib,
            "orig":       str(random.choice([0, 0, 0, 1, 4])),  # maioria origem nacional
        })

    return items, round(total_prod, 2), round(total_desc, 2), round(total_trib, 2)


def build_ide(inf_nfe: ET.Element, cuf: int, cnf_val: int, aamm: str,
              mod: int, serie: int, nnf: int, tpemis: int, chave: str,
              emit_date: datetime) -> None:
    sai_date = emit_date - timedelta(seconds=random.randint(30, 300))
    ide = ET.SubElement(inf_nfe, tag("ide"))

    for name, value in [
        ("cUF",      str(cuf)),
        ("cNF",      str(cnf_val).zfill(8)),
        ("natOp",    NAT_OP),
        ("indPag",   "0"),
        ("mod",      str(mod)),
        ("serie",    str(serie)),
        ("nNF",      str(nnf)),
        ("dhEmi",    emit_date.strftime("%Y-%m-%dT%H:%M:%S-03:00")),
        ("dhSaiEnt", sai_date.strftime("%Y-%m-%dT%H:%M:%S-03:00")),
        ("tpNF",     "1"),
        ("idDest",   "1"),
        ("cMunFG",   str(random.randint(1000000, 9999999))),
        ("tpImp",    "2"),
        ("tpEmis",   str(tpemis)),
        ("cDV",      chave[-1]),
        ("tpAmb",    "1"),
        ("finNFe",   "1"),
        ("indFinal", "1"),
        ("indPres",  "1"),
        ("procEmi",  "0"),
        ("verProc",  "NFe - 3.1.0-01"),
    ]:
        sub(ide, name, value)

    nfref = ET.SubElement(ide, tag("NFref"))
    ref_ecf = ET.SubElement(nfref, tag("refECF"))
    sub(ref_ecf, "mod", "2D")
    sub(ref_ecf, "nECF", "001")
    sub(ref_ecf, "nCOO", str(random.randint(100000, 999999)).zfill(6))


def build_emit(inf_nfe: ET.Element, cnpj: str, uf: str) -> None:
    emit_el = ET.SubElement(inf_nfe, tag("emit"))
    suffix  = random.choice(SUPERMARKET_SUFFIXES)
    # Usa sobrenome/nome para compor razão social estilo supermercado
    trade_name = f"{fake.last_name().upper()} {suffix}"[:60]
    corp_name  = f"{trade_name} LTDA"[:60]
    sub(emit_el, "CNPJ",  cnpj)
    sub(emit_el, "xNome", corp_name)
    sub(emit_el, "xFant", trade_name)

    ender = ET.SubElement(emit_el, tag("enderEmit"))
    sub(ender, "xLgr",   (fake.street_name() + " N " + str(random.randint(1, 9999))).upper()[:60])
    sub(ender, "nro",    str(random.randint(1, 9999)))
    sub(ender, "xBairro", fake.bairro().upper()[:60])
    sub(ender, "cMun",   str(random.randint(1000000, 9999999)))
    sub(ender, "xMun",   fake.city().upper()[:60])
    sub(ender, "UF",     uf)
    sub(ender, "CEP",    only_digits(fake.postcode())[:8].zfill(8))
    sub(ender, "cPais",  "1058")
    sub(ender, "xPais",  "BRASIL")
    sub(ender, "fone",   only_digits(fake.phone_number())[:12])

    sub(emit_el, "IE",   str(random.randint(100000000, 999999999)))
    sub(emit_el, "IM",   str(random.randint(1000000, 9999999)))
    sub(emit_el, "CNAE", CNAE_SUPERMERCADO)
    sub(emit_el, "CRT",  str(random.randint(1, 3)))


def build_dest(inf_nfe: ET.Element) -> None:
    dest_el = ET.SubElement(inf_nfe, tag("dest"))
    cpf = only_digits(fake.cpf())
    dest_uf = random.choice(list(UF_CODES.keys()))

    sub(dest_el, "CPF",   cpf)
    sub(dest_el, "xNome", fake.name().upper()[:60])

    ender = ET.SubElement(dest_el, tag("enderDest"))
    sub(ender, "xLgr",    fake.street_name().upper()[:60])
    sub(ender, "nro",     str(random.randint(1, 9999)))
    sub(ender, "xBairro", fake.bairro().upper()[:60])
    sub(ender, "cMun",    str(random.randint(1000000, 9999999)))
    sub(ender, "xMun",    fake.city().upper()[:60])
    sub(ender, "UF",      dest_uf)
    sub(ender, "CEP",     only_digits(fake.postcode())[:8].zfill(8))
    sub(ender, "cPais",   "1058")
    sub(ender, "xPais",   "BRASIL")
    sub(ender, "fone",    only_digits(fake.phone_number())[:11])

    sub(dest_el, "indIEDest", "9")
    sub(dest_el, "email",     fake.email())


def build_det(inf_nfe: ET.Element, items: list) -> None:
    for i, item in enumerate(items, 1):
        det = ET.SubElement(inf_nfe, tag("det"), {"nItem": str(i)})

        prod_el = ET.SubElement(det, tag("prod"))
        sub(prod_el, "cProd",   item["cProd"])
        ET.SubElement(prod_el, tag("cEAN"))
        sub(prod_el, "xProd",   item["xProd"])
        sub(prod_el, "NCM",     item["NCM"])
        sub(prod_el, "CFOP",    item["CFOP"])
        sub(prod_el, "uCom",    item["unit"])
        sub(prod_el, "qCom",    f"{item['qty']:.4f}")
        sub(prod_el, "vUnCom",  f"{item['unit_price']:.10f}")
        sub(prod_el, "vProd",   f"{item['v_prod']:.2f}")
        ET.SubElement(prod_el, tag("cEANTrib"))
        sub(prod_el, "uTrib",   item["unit"])
        sub(prod_el, "qTrib",   f"{item['qty']:.4f}")
        sub(prod_el, "vUnTrib", f"{item['unit_price']:.10f}")
        sub(prod_el, "vDesc",   f"{item['v_desc']:.2f}")
        sub(prod_el, "indTot",  "1")

        imposto_el = ET.SubElement(det, tag("imposto"))
        sub(imposto_el, "vTotTrib", f"{item['v_trib']:.2f}")

        icms_el  = ET.SubElement(imposto_el, tag("ICMS"))
        icmssn   = ET.SubElement(icms_el, tag("ICMSSN102"))
        sub(icmssn, "orig",  item["orig"])
        sub(icmssn, "CSOSN", "102")

        pis_el   = ET.SubElement(imposto_el, tag("PIS"))
        pis_outr = ET.SubElement(pis_el, tag("PISOutr"))
        sub(pis_outr, "CST",  "99")
        sub(pis_outr, "vBC",  "0.00")
        sub(pis_outr, "pPIS", "0.0000")
        sub(pis_outr, "vPIS", "0.00")

        cofins_el   = ET.SubElement(imposto_el, tag("COFINS"))
        cofins_outr = ET.SubElement(cofins_el, tag("COFINSOutr"))
        sub(cofins_outr, "CST",     "99")
        sub(cofins_outr, "vBC",     "0.00")
        sub(cofins_outr, "pCOFINS", "0.0000")
        sub(cofins_outr, "vCOFINS", "0.00")


def build_total(inf_nfe: ET.Element, total_prod: float,
                total_desc: float, total_trib: float) -> None:
    v_nf = round(total_prod - total_desc, 2)
    total_el  = ET.SubElement(inf_nfe, tag("total"))
    icms_tot  = ET.SubElement(total_el, tag("ICMSTot"))

    for name, value in [
        ("vBC",       "0.00"),
        ("vICMS",     "0.00"),
        ("vICMSDeson","0.00"),
        ("vBCST",     "0.00"),
        ("vST",       "0.00"),
        ("vProd",     f"{total_prod:.2f}"),
        ("vFrete",    "0.00"),
        ("vSeg",      "0.00"),
        ("vDesc",     f"{total_desc:.2f}"),
        ("vII",       "0.00"),
        ("vIPI",      "0.00"),
        ("vPIS",      "0.00"),
        ("vCOFINS",   "0.00"),
        ("vOutro",    "0.00"),
        ("vNF",       f"{v_nf:.2f}"),
        ("vTotTrib",  f"{total_trib:.2f}"),
    ]:
        sub(icms_tot, name, value)


def build_transp(inf_nfe: ET.Element) -> None:
    transp_el = ET.SubElement(inf_nfe, tag("transp"))
    sub(transp_el, "modFrete", str(random.randint(0, 4)))


def build_inf_adic(inf_nfe: ET.Element, total_trib: float,
                   total_prod: float, total_desc: float) -> None:
    v_nf = round(total_prod - total_desc, 2)
    inf_adic = ET.SubElement(inf_nfe, tag("infAdic"))
    ncoo = random.randint(10000, 99999)
    sub(inf_adic, "infAdFisco",
        f"Nota referente ao cupom fiscal: {ncoo};")
    vendedor = f"{random.randint(1, 30)}-{fake.name().upper()}"
    sub(inf_adic, "infCpl",
        f"Pedido: {random.randint(10000, 99999)} "
        f"Vendedor: {vendedor} "
        f"Condicao Pgto.: {random.choice(COND_PGTO_OPTIONS)} ;"
        f"EMPRESA OPTANTE PELO SIMPLES NACIONAL;"
        f"serial {random.randint(10000000000, 99999999999)};"
        f"Voce pagou aproximadamente: ;"
        f"R$ {total_trib:.2f} de tributos federais ;"
        f"R$ {v_nf:.2f} pelos produtos ;"
        f"Fonte: IBPT 5oi7eW. ;")


def build_prot_nfe(root: ET.Element, chave: str, emit_date: datetime) -> None:
    prot_nfe = ET.SubElement(root, tag("protNFe"), {"versao": "3.10"})
    inf_prot = ET.SubElement(prot_nfe, tag("infProt"))
    recbto = emit_date + timedelta(seconds=random.randint(30, 180))
    dig_val = base64.b64encode(
        hashlib.sha1(chave.encode()).digest()
    ).decode()

    for name, value in [
        ("tpAmb",    "1"),
        ("verAplic", "NFe - 3.1.0-01"),
        ("chNFe",    chave),
        ("dhRecbto", recbto.strftime("%Y-%m-%dT%H:%M:%S-03:00")),
        ("nProt",    str(random.randint(100000000000000, 999999999999999))),
        ("digVal",   dig_val),
        ("cStat",    "100"),
        ("xMotivo",  "Autorizado o uso da NF-e"),
    ]:
        sub(inf_prot, name, value)


def generate_nfe_xml() -> ET.Element:
    # Dados base da NF-e
    start_date = datetime(2018, 1, 1)
    end_date   = datetime(2025, 12, 31)
    emit_date  = start_date + timedelta(
        days=random.randint(0, (end_date - start_date).days),
        hours=random.randint(8, 18),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )

    uf       = random.choice(list(UF_CODES.keys()))
    cuf      = UF_CODES[uf]
    aamm     = emit_date.strftime("%y%m")
    cnpj     = only_digits(fake.cnpj())
    mod      = 55
    serie    = random.randint(1, 3)
    nnf      = random.randint(1000, 999999)
    tpemis   = 1
    cnf_val  = random.randint(10000000, 99999999)

    chave  = generate_nfe_key(cuf, aamm, cnpj, mod, serie, nnf, tpemis, cnf_val)
    nfe_id = f"NFe{chave}"

    num_items = random.randint(1, 5)
    items, total_prod, total_desc, total_trib = build_items(num_items)

    # Montar XML
    root    = ET.Element(tag("nfeProc"), {"versao": "3.10"})
    nfe_el  = ET.SubElement(root, tag("NFe"))
    inf_nfe = ET.SubElement(nfe_el, tag("infNFe"), {"versao": "3.10", "Id": nfe_id})

    build_ide(inf_nfe, cuf, cnf_val, aamm, mod, serie, nnf, tpemis, chave, emit_date)
    build_emit(inf_nfe, cnpj, uf)
    build_dest(inf_nfe)
    build_det(inf_nfe, items)
    build_total(inf_nfe, total_prod, total_desc, total_trib)
    build_transp(inf_nfe)
    build_inf_adic(inf_nfe, total_trib, total_prod, total_desc)
    build_prot_nfe(root, chave, emit_date)

    return root


def prettify_xml(element: ET.Element) -> str:
    raw    = ET.tostring(element, encoding="unicode", xml_declaration=False)
    dom    = parseString(raw)
    pretty = dom.toprettyxml(indent="\t", encoding="UTF-8")
    return pretty.decode("utf-8")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Gerando 100 arquivos XML de NF-e fake em '{OUTPUT_DIR}/'...")

    for i in range(1, 101):
        root        = generate_nfe_xml()
        xml_content = prettify_xml(root)
        filename    = os.path.join(OUTPUT_DIR, f"nfe_{i:03d}.xml")

        with open(filename, "w", encoding="utf-8") as f:
            f.write(xml_content)

        print(f"  [{i:3d}/100] {filename}")

    print(f"\nConcluido! 100 arquivos gerados em '{OUTPUT_DIR}/'")


if __name__ == "__main__":
    main()
