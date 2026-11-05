"""
Verifica que los conteos cuadren y responde una pregunta con las ventanas guardadas.

    python verificar_conteos.py                 # lee resumen_productor.json
    python verificar_conteos.py 3312            # o indica el total publicado a mano
"""
import json
import sys
from collections import Counter, defaultdict
from decimal import Decimal

import boto3

REGION = "us-east-1"
ddb = boto3.resource("dynamodb", region_name=REGION)


def escanear(nombre):
    t = ddb.Table(nombre)
    items, kw = [], {}
    while True:
        r = t.scan(**kw)
        items += r["Items"]
        if "LastEvaluatedKey" not in r:
            return items
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]


vistos = escanear("t2_eventos_vistos")
repetidos = escanear("t2_eventos_repetidos")
invalidos = escanear("t2_eventos_invalidos")
ventanas = escanear("t2_ventanas")

unicos = len(vistos)
suma_conteos = int(sum(v["conteo"] for v in ventanas))

if len(sys.argv) > 1:
    publicados = int(sys.argv[1])
else:
    with open("resumen_productor.json") as f:
        publicados = json.load(f)["publicados"]

print("===== CONTEOS =====")
print(f"Publicados (productor)        : {publicados}")
print(f"Unicos procesados             : {unicos}   (suma de 'conteo' en ventanas: {suma_conteos})")
print(f"Repetidos descartados         : {len(repetidos)}")
print(f"Invalidos descartados         : {len(invalidos)}")
total = unicos + len(repetidos) + len(invalidos)
print(f"Unicos + repetidos + invalidos: {total}")
print("CUADRA" if total == publicados and suma_conteos == unicos
      else f"NO CUADRA (diferencia {publicados - total})")

print("\n===== MOTIVOS DE DESCARTE (invalidos) =====")
for motivo, n in Counter(i["motivo"] for i in invalidos).most_common():
    print(f"  {motivo}: {n}")

print("\n===== RESULTADO POR VENTANA (primeras 10) =====")
print(f"{'categoria':<14}{'ventana (UTC)':<18}{'conteo':>7}{'promedio':>10}")
for v in sorted(ventanas, key=lambda x: (x["ventana"], x["categoria"]))[:10]:
    print(f"{v['categoria']:<14}{v['ventana']:<18}{int(v['conteo']):>7}{float(v['promedio']):>10.2f}")

print("\n===== PREGUNTA: que categoria tiene el mayor valor promedio por evento? =====")
tot = defaultdict(lambda: [0, Decimal(0)])
for v in ventanas:
    tot[v["categoria"]][0] += int(v["conteo"])
    tot[v["categoria"]][1] += v["suma_valor"]
for cat, (n, s) in sorted(tot.items(), key=lambda kv: kv[1][1] / kv[1][0], reverse=True):
    print(f"  {cat:<14} eventos: {n:<6} promedio: {float(s / n):.2f}")

mejor = max(ventanas, key=lambda x: int(x["conteo"]))
print(f"\nVentana con mas eventos: {mejor['categoria']} a las {mejor['ventana']} UTC "
      f"({int(mejor['conteo'])} eventos)")
