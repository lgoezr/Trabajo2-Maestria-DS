"""
Aumenta orders.csv (1.000.000 filas) a ~5.000.000 filas. Se corre LOCALMENTE, antes de subir a S3.

Tecnica de aumento (declarala en el video e informe):
  1. Se replica el dataset 5 veces. En cada copia k>=1 se agrega el sufijo "-k" al order_id
     (ids nuevos) y se perturba unit_price y order_amount con el MISMO factor aleatorio (+-10 %)
     para conservar la relacion entre ambos. customer_id NO cambia (asi sigue uniendo con customers.csv).
  2. Se inyecta ruido controlado en cada copia para que la limpieza tenga algo que corregir:
       0,5 % customer_id vacio | 0,3 % fecha invalida | 0,3 % monto negativo | 0,5 % filas duplicadas

Uso:  python 01_aumentar_datos.py ruta/orders.csv
Salida: carpeta orders_aug/ con orders_part_1.csv ... orders_part_5.csv (subir a s3://BUCKET/raw/orders/)
"""
import os
import sys

import numpy as np
import pandas as pd

entrada = sys.argv[1] if len(sys.argv) > 1 else "orders.csv"
np.random.seed(42)

df = pd.read_csv(entrada, dtype=str, keep_default_na=False)
print("Filas originales:", len(df))
os.makedirs("orders_aug", exist_ok=True)

total = 0
for k in range(5):
    d = df.copy()
    if k > 0:
        d["order_id"] = d["order_id"] + f"-{k}"
        f = np.random.uniform(0.9, 1.1, len(d))
        for col in ["unit_price", "order_amount"]:
            d[col] = (pd.to_numeric(d[col], errors="coerce") * f).round(2).astype(str)

    n = len(d)
    d.loc[np.random.choice(n, int(n * 0.005), replace=False), "customer_id"] = ""
    d.loc[np.random.choice(n, int(n * 0.003), replace=False), "order_date"] = "fecha-invalida"
    d.loc[np.random.choice(n, int(n * 0.003), replace=False), "order_amount"] = "-1"
    d = pd.concat([d, d.sample(frac=0.005, random_state=k)])

    d.to_csv(f"orders_aug/orders_part_{k + 1}.csv", index=False)
    total += len(d)
    print(f"orders_part_{k + 1}.csv: {len(d)} filas")

print("TOTAL filas:", total)
