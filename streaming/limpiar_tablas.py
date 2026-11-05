"""
Vacia las 4 tablas de la Parte B (NO las borra). Usalo despues de una prueba corta
y antes de la corrida final, para que los conteos partan de cero.
    python limpiar_tablas.py
"""
import boto3

ddb = boto3.resource("dynamodb", region_name="us-east-1")
TABLAS = {
    "t2_ventanas": ["categoria", "ventana"],
    "t2_eventos_vistos": ["event_id"],
    "t2_eventos_invalidos": ["registro_id"],
    "t2_eventos_repetidos": ["registro_id"],
}

for nombre, llaves in TABLAS.items():
    t = ddb.Table(nombre)
    n, kw = 0, {}
    while True:
        r = t.scan(ProjectionExpression=", ".join(f"#k{i}" for i in range(len(llaves))),
                   ExpressionAttributeNames={f"#k{i}": k for i, k in enumerate(llaves)}, **kw)
        with t.batch_writer() as b:
            for item in r["Items"]:
                b.delete_item(Key={k: item[k] for k in llaves})
                n += 1
        if "LastEvaluatedKey" not in r:
            break
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]
    print(f"{nombre}: {n} items borrados")
