"""
Productor continuo para Kinesis Data Streams (Trabajo 2, Parte B).

Uso:
    python productor.py                      # 330 s (5,5 min) a 10 eventos/s
    python productor.py --segundos 20        # prueba corta

Publica eventos con: event_id, categoria, valor, event_ts (ms), usuario.
A PROPOSITO envia:
  - ~4 % de eventos REPETIDOS (mismo event_id, categoria, valor y event_ts de uno ya enviado)
  - ~4 % de eventos INVALIDOS (sin categoria, o valor fuera de rango [1, 1000])
Cada registro termina en salto de linea para que el consumidor de S3 guarde JSON por lineas.
"""
import argparse
import json
import random
import time
import uuid

import boto3

# Cambia estas categorias por valores reales de product_category de tu proyecto
CATEGORIAS = ["Electronics", "Home", "Beauty", "Fashion", "Grocery"]
PROB_REPETIDO = 0.04
PROB_INVALIDO = 0.04

p = argparse.ArgumentParser()
p.add_argument("--stream", default="t2-eventos-stream")
p.add_argument("--region", default="us-east-1")
p.add_argument("--segundos", type=int, default=330)
p.add_argument("--tasa", type=int, default=10, help="eventos por segundo")
args = p.parse_args()

kinesis = boto3.client("kinesis", region_name=args.region)


def evento_nuevo():
    return {
        "event_id": str(uuid.uuid4()),
        "categoria": random.choice(CATEGORIAS),
        "valor": round(random.uniform(5, 500), 2),
        "event_ts": int(time.time() * 1000),
        "usuario": f"U{random.randint(1, 2000):05d}",
    }


def evento_invalido():
    ev = evento_nuevo()
    if random.random() < 0.5:
        del ev["categoria"]                              # campo faltante
    else:
        ev["valor"] = random.choice([-50, 0, 5000, 99999])   # fuera de rango
    return ev


def enviar(eventos):
    """Envia una lista de (evento, tipo). Devuelve las que Kinesis acepto."""
    aceptados = []
    for i in range(0, len(eventos), 500):
        pendientes = eventos[i:i + 500]
        for intento in range(6):
            recs = [{"Data": (json.dumps(e) + "\n").encode("utf-8"),
                     "PartitionKey": e["event_id"]} for e, _ in pendientes]
            r = kinesis.put_records(StreamName=args.stream, Records=recs)
            fallidos = []
            for par, res in zip(pendientes, r["Records"]):
                if "ErrorCode" in res:
                    fallidos.append(par)
                else:
                    aceptados.append(par)
            pendientes = fallidos
            if not pendientes:
                break
            time.sleep(0.5 * (intento + 1))
        if pendientes:
            print(f"AVISO: {len(pendientes)} registros no se pudieron publicar")
    return aceptados


cont = {"unicos": 0, "repetidos": 0, "invalidos": 0}
historial = []          # validos ya publicados (para repetirlos despues)
t0 = time.time()
sig_reporte = 30
print(f"Publicando en '{args.stream}' durante {args.segundos} s a {args.tasa} eventos/s ...")

while time.time() - t0 < args.segundos:
    inicio = time.time()
    lote, nuevos = [], []
    for _ in range(args.tasa):
        x = random.random()
        if x < PROB_INVALIDO:
            lote.append((evento_invalido(), "invalidos"))
        elif x < PROB_INVALIDO + PROB_REPETIDO and historial:
            lote.append((dict(random.choice(historial)), "repetidos"))
        else:
            ev = evento_nuevo()
            lote.append((ev, "unicos"))
            nuevos.append(ev)

    for ev, tipo in enviar(lote):
        cont[tipo] += 1
        if tipo == "unicos":
            historial.append(ev)
    historial = historial[-500:]

    if time.time() - t0 >= sig_reporte:
        print(f"  {int(time.time() - t0)} s | publicados: {sum(cont.values())}", flush=True)
        sig_reporte += 30
    time.sleep(max(0, 1 - (time.time() - inicio)))

total = sum(cont.values())
resumen = {"publicados": total, **cont, "duracion_s": round(time.time() - t0, 1)}
print("\n===== RESUMEN DEL PRODUCTOR =====")
print("Publicados totales:", total)
print("  unicos   :", cont["unicos"])
print("  repetidos:", cont["repetidos"])
print("  invalidos:", cont["invalidos"])
print("Duracion (s):", resumen["duracion_s"])
with open("resumen_productor.json", "w") as f:
    json.dump(resumen, f)
