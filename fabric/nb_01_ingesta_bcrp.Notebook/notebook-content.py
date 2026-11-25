# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "09a5682f-1f0d-4c70-864e-d52c019d6425",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "e4605945-f370-426b-b894-1c71c815e95b",
# META       "known_lakehouses": [
# META         {
# META           "id": "09a5682f-1f0d-4c70-864e-d52c019d6425"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests, json
from datetime import date, datetime, timezone

SERIES = {
    "PD04637PD": "tc_compra",   # TC interbancario - compra
    "PD04638PD": "tc_venta",    # TC interbancario - venta
}
FECHA_INI = "2022-01-01"
FECHA_FIN = date.today().isoformat()
BASE = "https://estadisticas.bcrp.gob.pe/estadisticas/series/api"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fetch_bcrp(codigos, ini, fin):
    url = f"{BASE}/{'-'.join(codigos)}/json/{ini}/{fin}"
    r = requests.get(url, timeout=60, headers={"User-Agent": "observatorio-peru/1.0"})
    r.raise_for_status()
    payload = json.loads(r.content.decode("utf-8-sig"))  # el BCRP a veces manda BOM
    return url, payload

url, payload = fetch_bcrp(list(SERIES), FECHA_INI, FECHA_FIN)

print(url)
print([s["name"] for s in payload["config"]["series"]])
print(len(payload["periods"]), "periodos")
payload["periods"][:3]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os

ingest_ts = datetime.now(timezone.utc)
ruta = f"/lakehouse/default/Files/bcrp/tipo_cambio/ingest_date={ingest_ts:%Y-%m-%d}"
os.makedirs(ruta, exist_ok=True)

archivo = f"{ruta}/raw_{ingest_ts:%H%M%S}.json"
with open(archivo, "w", encoding="utf-8") as f:
    json.dump(payload, f, ensure_ascii=False)

print("Guardado:", archivo)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

columnas = list(SERIES.values())          # ['tc_compra', 'tc_venta'], mismo orden que la API
filas = [
    {"periodo_raw": p["name"], **dict(zip(columnas, p["values"]))}
    for p in payload["periods"]
]

df = (
    spark.createDataFrame(filas)
    .select("periodo_raw", *columnas)
    .withColumn("_source_url", F.lit(url))
    .withColumn("_ingested_at", F.lit(ingest_ts.isoformat()).cast("timestamp"))
)

df.write.mode("overwrite").format("delta").saveAsTable("bcrp_tipo_cambio")
display(df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT COUNT(*)                                        AS filas,
           SUM(CASE WHEN tc_venta = 'n.d.' THEN 1 ELSE 0 END) AS venta_nd,
           MAX(_ingested_at)                               AS ultima_ingesta
    FROM bcrp_tipo_cambio
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SELECT periodo_raw FROM bcrp_tipo_cambio WHERE tc_venta = 'n.d.'").show(62, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
